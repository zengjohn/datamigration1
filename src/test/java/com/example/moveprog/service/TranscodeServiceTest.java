package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.exception.JobStoppedException;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.example.moveprog.repository.QianyiRepository;
import com.example.moveprog.util.FastEscapeHandler;
import com.example.moveprog.util.MigrationOutputDirectorUtil;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TranscodeServiceTest {
    private static final String CHARSET_IBM = "x-IBM1388";

    @Mock private StateManager stateManager;
    @Mock private MigrationJobRepository jobRepository;
    @Mock private QianyiRepository qianyiRepo;
    @Mock private QianyiDetailRepository detailRepo;
    @Mock private CsvSplitRepository splitRepo;
    @Mock private JobControlManager jobControlManager;
    @Mock private TargetDatabaseConnectionManager targetDatabaseConnectionManager;
    @Mock private MigrationArtifactManager migrationArtifactManager;
    @Mock private AppProperties config;

    @InjectMocks
    private TranscodeService transcodeService;

    // JUnit 5 临时目录，测试结束后自动删除
    @TempDir
    Path tempDir;

    // 静态方法的 Mock 对象
    private MockedStatic<SchemaParseUtil> schemaParseUtilMock;
    private MockedStatic<FastEscapeHandler> fastEscapeHandlerMock;

    @BeforeEach
    void setUp() {
        schemaParseUtilMock = Mockito.mockStatic(SchemaParseUtil.class);
        fastEscapeHandlerMock = Mockito.mockStatic(FastEscapeHandler.class);
    }

    @AfterEach
    void tearDown() {
        schemaParseUtilMock.close();
        fastEscapeHandlerMock.close();
    }

    @Data
    @Builder
    @ToString
    static class CsvScenario {
        String description;    // 场景描述
        boolean hasHeader;     // 是否有表头
        long skipRows;          // 配置跳过行数
        int totalDataRows;     // 实际数据行数（不含Header）
        int splitSize;         // 拆分大小
        String lineEnding;     // 换行符 (\r\n 或 \n)

        /**
         * 预期生成的切片文件数
         */
        int expectedSplitFiles;
        /**
         * 预期入库记录数
         */
        long expectedDbRecords;
        /**
         * 期望的每个拆分文件行数
         */
        List<Integer> expectedSplitCounts;
        /**
         * 每个拆分文件第一个行在源文件中的 第N行(从1开始)
         */
        List<Integer> expectedSplitStartNo;
    }

    static Stream<Arguments> provideCsvScenarios() {
        return Stream.of(
                // 场景 1: 标准情况 (无头，不跳过，不拆分)
                Arguments.of(
                        CsvScenario.builder()
                                .description("Happy Path: No Header, No Skip, No Split")
                                .hasHeader(false).skipRows(0).totalDataRows(10).splitSize(100).lineEnding("\r\n")
                                .expectedSplitFiles(1).expectedDbRecords(10)
                                .expectedSplitCounts(Arrays.asList(10))
                                .expectedSplitStartNo(Arrays.asList(1))
                                .build()),

                // 场景 2: 有表头，需要跳过 1 行
                Arguments.of(
                        CsvScenario.builder()
                                .description("With Header: Skip 1 row")
                                .hasHeader(true).skipRows(1).totalDataRows(10).splitSize(100).lineEnding("\n")
                                .expectedSplitFiles(1).expectedDbRecords(10)
                                .expectedSplitCounts(Arrays.asList(9))
                                .expectedSplitStartNo(Arrays.asList(3)) // utf8 csv第一行在源ibm csv中是第三行
                                .build()), // skip掉header，读10行数据

                // 场景 3: 拆分测试 (10条数据，每3条拆一个文件 -> 4个文件)
                Arguments.of(
                        CsvScenario.builder()
                                .description("Splitting: 10 rows, split size 3")
                                .hasHeader(false).skipRows(0).totalDataRows(10).splitSize(3).lineEnding("\r\n")
                                .expectedSplitFiles(4).expectedDbRecords(10)
                                .expectedSplitCounts(Arrays.asList(3, 3, 3, 1))
                                .expectedSplitStartNo(Arrays.asList(1, 4, 7, 10))
                                .build()), // 3+3+3+1 = 4 files

                // 场景 4: 有表头，需要跳过 3 行, 每3行一个拆分
                Arguments.of(
                        CsvScenario.builder()
                                .description("With Header: Skip 3 row, 每3条一个拆分")
                                .hasHeader(true).skipRows(3).totalDataRows(10).splitSize(3).lineEnding("\n")
                                .expectedSplitFiles(3).expectedDbRecords(7)
                                .expectedSplitCounts(Arrays.asList(3, 3, 1))
                                .expectedSplitStartNo(Arrays.asList(5, 8, 11)) // utf8 csv第一行在源ibm csv中是第三行
                                .build()),

                // 场景 5: 只有 Header 没有数据
                Arguments.of(
                        CsvScenario.builder()
                                .description("Header Only, No Data")
                                .hasHeader(true).skipRows(1).totalDataRows(0).splitSize(100).lineEnding("\n")
                                .expectedSplitFiles(0).expectedDbRecords(0)
                                .expectedSplitCounts(Arrays.asList(0))
                                .expectedSplitStartNo(Arrays.asList(0))
                                .build())
        );
    }

    @ParameterizedTest(name = "{index} {0}") // 显示 Scenario 的 toString 或 description
    @MethodSource("provideCsvScenarios")
    void testExecute_Variations(CsvScenario scenario) throws IOException {
        // --- 1. 动态准备数据 ---
        Long jobId = 100L;
        Long qianyiId = 10L;
        Long detailId = 1L;

        // 1.1 根据场景动态生成 CSV 内容
        Path sourceFile = tempDir.resolve("source_ibm.csv");
        StringBuilder csvContent = new StringBuilder();

        // 写入数据行
        List<List<String>> rows = new ArrayList<>();

        // 如果有 Header，先写入 Header
        if (scenario.hasHeader) {
            String header = "id,name,remark,weight,salary,birth,birthday,lunchtime";
            csvContent.append(header);
            csvContent.append(scenario.lineEnding);
            rows.add(List.of(header.split(",")));
        }

        for(int i=0; i < scenario.totalDataRows; i++) {
            List<String> row = generateRowData(i + 15L);
            rows.add(row);
            csvContent.append(row.stream().collect(Collectors.joining(",")));
            if (i < scenario.totalDataRows - 1) {
                csvContent.append(scenario.lineEnding);
            }
        }
        writeWithEncoding(sourceFile.toString(), csvContent.toString(), CHARSET_IBM);

        // 模拟输出目录结构 (outDirectory 现在来自 MigrationJob)
        Path outDir = tempDir.resolve("output_home");

        // --- 2. Mock 实体 ---
        MigrationJob job = new MigrationJob();
        job.setId(jobId);
        job.setOutDirectory(outDir.toString());

        Qianyi qianyi = new Qianyi();
        qianyi.setId(qianyiId);
        qianyi.setJobId(jobId);
        qianyi.setDdlFilePath("schema.sql"); // 假路径

        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setQianyiId(qianyiId);
        detail.setJobId(jobId);
        detail.setSourceCsvPath(sourceFile.toString());
        detail.setStatus(DetailStatus.TRANSCODING); // 初始状态

        // --- 3. Mock Repository ---
        // 注意：代码中有两次 findById(detailId)，一次是清理旧数据，一次是双重检查
        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));
        when(qianyiRepo.findById(qianyiId)).thenReturn(Optional.of(qianyi));
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        // ---  动态 Mock 配置 ---
        mockAppConfig(scenario);

        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(anyString()))
                .thenReturn(Collections.emptyList());

        transcodeService.execute(detailId);

        for (int i = 0; i < scenario.expectedSplitFiles; i++) {
            Path expectedSplitFile = Paths.get(MigrationOutputDirectorUtil.transcodeSplitFile(job, qianyiId, detailId, i+1));
            assertTrue(Files.exists(expectedSplitFile), "拆分文件 " + (i+1) + " 应该存在");

            List<String> fileContents = Files.readAllLines(expectedSplitFile, StandardCharsets.UTF_8);
            Integer splitCounts = scenario.getExpectedSplitCounts().get(i);
            assertEquals(splitCounts, fileContents.size());

            // 验证开始行
            Integer srcCsvStartNo = scenario.getExpectedSplitStartNo().get(i);
            {
                List<String> row = rows.get(srcCsvStartNo-1);
                String[] splits = fileContents.get(0).split(",");
                for (int j = 0; j < row.size(); j++) {
                    assertEquals("\"" + row.get(j) + "\"", splits[j]);
                }
                assertEquals("\"" + srcCsvStartNo + "\"", splits[splits.length - 1]); // 最后一列是源文件(csv)行号
            }

            // 验证所有行
            for(int irow=0; irow < fileContents.size(); irow++) {
                String[] splits = fileContents.get(0).split(",");
                String csvLineNo = splits[splits.length - 1].replace("\"",""); // 最后一列是源文件(csv)行号
                List<String> row = rows.get(Integer.valueOf(csvLineNo)-1);
                for (int j = 0; j < row.size(); j++) {
                    assertEquals("\"" + row.get(j) + "\"", splits[j]);
                }
            }

        }

        // 调用清除
        verify(migrationArtifactManager).cleanTranscodeArtifacts(eq(detail));
        try {
            verify(targetDatabaseConnectionManager).deleteSplitsAndLoadData(eq(detailId));
        } catch (Exception e) {
            fail("Exception verifying db call");
        }
        verify(splitRepo).deleteByDetailId(eq(detailId));
        verify(detailRepo, atLeastOnce()).updateSourceRowCount(eq(detailId), any());

        // 验证数据库保存次数 (总的 save 调用次数应该等于切片文件数)
        verify(splitRepo, times(scenario.expectedSplitFiles)).save(any(CsvSplit.class));
        // 验证处理总状态
        verify(stateManager).updateDetailStatus(eq(detailId), eq(DetailStatus.PROCESSING_CHILDS));

        verify(detailRepo).updateErrorCount(eq(detailId), eq(0L));
    }

    private List<String> generateRowData(Long id) {
        List<String> rowData = new ArrayList<>();
        rowData.add(id.toString());
        rowData.add("张三_"+id);
        rowData.add("53.12");
        rowData.add("31215.00");
        rowData.add("1980-01-05");
        rowData.add("1980-01-05 13:15:16.123456");
        rowData.add("12:15:13");
        return rowData;
    }

    @Test
    @DisplayName("源ibm1388 csv文件有错误编码")
    void testExecute_WithErrorEncode() throws IOException {
        // --- 1. 动态准备数据 ---
        Long jobId = 100L;
        Long qianyiId = 10L;
        Long detailId = 1L;

        Path sourceFile = tempDir.resolve("source_ibm.csv");

        // 写入数据行
        int totalDataRows = 10;
        List<Integer> errorCodeLines = List.of(2,6,8);
        List<List<String>> rows = new ArrayList<>();
        for(int i=0; i < totalDataRows; i++) {
            List<String> row = generateRowData(i + 15L);
            rows.add(row);

            StringBuilder csvContent = new StringBuilder();
            csvContent.append(row.stream().collect(Collectors.joining(",")));
            csvContent.append("\n");
            appendWithEncoding(sourceFile.toString(), csvContent.toString(),
                    errorCodeLines.contains(i) ? "UTF8" : CHARSET_IBM);
        }
        List<String> allLines = Files.readAllLines(sourceFile);
        assertEquals(totalDataRows, allLines.size());

        // 模拟输出目录结构 (outDirectory 现在来自 MigrationJob)
        Path outDir = tempDir.resolve("output_home");

        // --- 2. Mock 实体 ---
        MigrationJob job = new MigrationJob();
        job.setId(jobId);
        job.setOutDirectory(outDir.toString());

        Qianyi qianyi = new Qianyi();
        qianyi.setId(qianyiId);
        qianyi.setJobId(jobId);
        qianyi.setDdlFilePath("schema.sql"); // 假路径

        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setQianyiId(qianyiId);
        detail.setJobId(jobId);
        detail.setSourceCsvPath(sourceFile.toString());
        detail.setStatus(DetailStatus.TRANSCODING); // 初始状态

        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));
        when(qianyiRepo.findById(qianyiId)).thenReturn(Optional.of(qianyi));
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        mockAppConfig("IBM1388", 2, false);

        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(anyString()))
                .thenReturn(Collections.emptyList());

        transcodeService.execute(detailId);

        Path expectedSplitFile = Paths.get(MigrationOutputDirectorUtil.transcodeSplitFile(job, qianyiId, detailId, 1));
        assertTrue(Files.exists(expectedSplitFile), "拆分文件存在");
        List<String> fileContents = Files.readAllLines(expectedSplitFile, StandardCharsets.UTF_8);
        assertEquals(totalDataRows-errorCodeLines.size(), fileContents.size());

        for(int irow=0; irow < totalDataRows; irow++) {
            String[] splits = fileContents.get(0).split(",");
            String csvLineNo = splits[splits.length - 1].replace("\"",""); // 最后一列是源文件(csv)行号
            List<String> row = rows.get(Integer.valueOf(csvLineNo)-1);
            for (int j = 0; j < row.size(); j++) {
                assertEquals("\"" + row.get(j) + "\"", splits[j]);
            }
        }

        Path errorFile = Paths.get(MigrationOutputDirectorUtil.transcodeErrorFile(job, qianyiId, detailId));
        assertTrue(Files.exists(errorFile), "转码失败文件存在");

        // 验证数据库保存次数 (总的 save 调用次数应该等于切片文件数)
        verify(splitRepo).save(any(CsvSplit.class));
        // 验证处理总状态
        verify(stateManager).updateDetailStatus(eq(detailId), eq(DetailStatus.PROCESSING_CHILDS));

        verify(detailRepo).updateErrorCount(eq(detailId), eq(0L));
    }

    /**
     * 测试清理旧数据逻辑
     */
    @Test
    void testCleanUpOldSplits() {
        Long jobId = 100L;
        Long detailId = 1L;

        Path outDir = tempDir.resolve("output_home");

        // Mock 实体
        MigrationJob job = new MigrationJob();
        job.setId(jobId);
        job.setOutDirectory(outDir.toString());

        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setJobId(jobId);

        // Mock Repo
        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));

        // 执行清理
        transcodeService.cleanUpOldSplits(detailId);

        // 验证
        verify(migrationArtifactManager).cleanTranscodeArtifacts(detail);
        try {
            verify(targetDatabaseConnectionManager).deleteSplitsAndLoadData(detailId);
        } catch (Exception e) {
            fail("Exception verifying db call");
        }
        verify(splitRepo).deleteByDetailId(detailId);

        verify(detailRepo).updateSourceRowCount(eq(detailId), eq(0L));
    }

    /**
     * 测试任务中断异常处理
     */
    @Test
    void testExecute_JobStopped() throws IOException {
        Long detailId = 1L;
        Long qianyiId = 100L;
        Long jobId = 100L;

        Path sourceFile = tempDir.resolve("source.csv");
        Files.createFile(sourceFile);

        Qianyi qianyi = new Qianyi();
        qianyi.setId(qianyiId);
        qianyi.setJobId(jobId);

        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setQianyiId(qianyiId);
        detail.setJobId(jobId);
        detail.setSourceCsvPath(sourceFile.toString());
        detail.setStatus(DetailStatus.TRANSCODING);

        mockAppConfig("IBM1388", 50, false);

        // 1. findById 必须成功，才能进入 try 块
        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));
        when(qianyiRepo.findById(qianyiId)).thenReturn(Optional.of(qianyi));

        // 2. 在 try 块内部抛出 JobStoppedException
        when(jobRepository.findById(jobId)).thenThrow(new JobStoppedException("Stop by user"));

        transcodeService.execute(detailId);

        // 验证：异常被捕获，没有抛出到外面，也没有更新为 FAIL
        verify(stateManager, never()).updateDetailStatus(eq(detailId), eq(DetailStatus.FAIL_TRANSCODE));
    }

    // 重载版本：专门为参数化测试服务
    private void mockAppConfig(CsvScenario scenario) {
        // 1. 创建 IBM 配置 (源端)
        AppProperties.CsvDetailConfig ibmConfig = new AppProperties.CsvDetailConfig();
        ibmConfig.setEncoding("IBM-1388");

        // 【核心修改】将 Scenario 中的动态参数填入配置对象
        ibmConfig.setSkipRows(scenario.skipRows);       // 设置跳过行数
        ibmConfig.setHeaderExtraction(scenario.hasHeader);         // 设置是否有表头
        ibmConfig.setLineSeparator(scenario.lineEnding);

        // 2. 创建 UTF-8 配置 (目标端) - 保持默认
        AppProperties.CsvDetailConfig utf8Config = new AppProperties.CsvDetailConfig();
        utf8Config.setEncoding("UTF-8");
        // 如果 scenario 里有关于 utf8 的配置也可以在这里 set

        // 3. 组装 CSV 配置
        AppProperties.Csv csv = new AppProperties.Csv();
        csv.setIbmSource(ibmConfig);
        csv.setUtf8Split(utf8Config);

        // 4. 创建性能配置 (Performance)
        AppProperties.Performance perf = new AppProperties.Performance();
        perf.setReadBufferSize(1024);
        perf.setWriteBufferSize(1024);
        perf.setSplitRows(scenario.splitSize);

        AppProperties.Transcode transcodeConfig = new AppProperties.Transcode();
        transcodeConfig.setMaxErrorCount(10);

        // 5. 将这些对象注入到主 Mock (config) 中
        // 注意：config 是类级别的 @Mock 变量
        lenient().when(config.getCsv()).thenReturn(csv);
        lenient().when(config.getPerformance()).thenReturn(perf);
        lenient().when(config.getTranscode()).thenReturn(transcodeConfig);
    }

    // --- 辅助：组装 AppProperties ---
    private void mockAppConfig(String encoding, int maxErrorCount, boolean tunneling) {
        // 创建配置层级
        AppProperties.CsvDetailConfig ibmConfig = new AppProperties.CsvDetailConfig();
        ibmConfig.setEncoding(encoding);
        ibmConfig.setTunneling(tunneling);
        // 默认 Parser 设置
        // ... (如果代码里有 toParserSettings 调用，确保 config 里的值不是 null)

        AppProperties.CsvDetailConfig utf8Config = new AppProperties.CsvDetailConfig();
        utf8Config.setEncoding("UTF-8");

        AppProperties.Csv csv = new AppProperties.Csv();
        csv.setIbmSource(ibmConfig);
        csv.setUtf8Split(utf8Config);

        AppProperties.Performance perf = new AppProperties.Performance();
        perf.setReadBufferSize(1024);
        perf.setWriteBufferSize(1024);
        perf.setSplitRows(100);

        AppProperties.Transcode jobConfig = new AppProperties.Transcode();
        jobConfig.setMaxErrorCount(maxErrorCount);

        // Mock config 行为
        lenient().when(config.getCsv()).thenReturn(csv);
        lenient().when(config.getPerformance()).thenReturn(perf);
        lenient().when(config.getTranscode()).thenReturn(jobConfig);
    }

    private static void writeWithEncoding(String path, String content, String encoding) throws IOException {
        File file = new File(path);
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file), encoding))) {
            writer.write(content);
            System.out.println("生成成功: " + path);
            System.out.println("文件大小: " + file.length() + " 字节");
        }
    }

    private static void appendWithEncoding(String path, String content, String encoding) throws IOException {
        File file = new File(path);
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file, true), encoding))) {
            writer.write(content);
        }
    }

}