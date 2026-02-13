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
import com.example.moveprog.util.MigrationOutputDirectorUtil;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
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

    @BeforeEach
    void setUp() {
        schemaParseUtilMock = Mockito.mockStatic(SchemaParseUtil.class);
    }

    @AfterEach
    void tearDown() {
        schemaParseUtilMock.close();
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
            List<String> row = generateRowData(i + 125L);
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
                Pair<List<String>,Integer> pair = parseUtf8Row(fileContents.get(0));
                assertIbm1388AndUtf8RowEquals(row, pair);
            }

            // 验证所有行
            for(int irow=0; irow < fileContents.size(); irow++) {
                Pair<List<String>,Integer> pair = parseUtf8Row(fileContents.get(irow));
                List<String> row = rows.get(pair.getValue()-1);
                assertIbm1388AndUtf8RowEquals(row, pair);
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

    /**
     * 验证行
     * @param actualRow 实际数据行
     * @param utf8RowPair utf8拆分文件行
     */
    private void assertIbm1388AndUtf8RowEquals(List<String> actualRow, Pair<List<String>,Integer> utf8RowPair) {
        List<String> columnValues = utf8RowPair.getKey();
        assertEquals(columnValues.size(), actualRow.size());
        for (int j = 0; j < actualRow.size(); j++) {
            assertEquals(actualRow.get(j), columnValues.get(j),
                    "列: " + j + " 期望(产生值): "+actualRow.get(j)+", 实际utf8文件值: "+columnValues.get(j));
        }
    }

    /**
     * 解析utf8拆分文件行
     * @param utf8Row
     * @return
     */
    private Pair<List<String>, Integer> parseUtf8Row(String utf8Row) {
        String[] splits = utf8Row.split(",");
        String csvLineNo = unquotedUtf8Row(splits[splits.length - 1]); // 最后一列是源文件(csv)行号
        List<String> columns = new ArrayList<>();
        for (int j = 0; j < splits.length-1; j++) {
            columns.add(unquotedUtf8Row(splits[j]));
        }
        return Pair.of(columns, Integer.valueOf(csvLineNo));
    }

    private String unquotedUtf8Row(String row) {
        return row.substring(1, row.length() - 1);
    }

    private List<String> generateRowData(Long id) {
        List<String> rowData = new ArrayList<>();
        rowData.add(id.toString());
        rowData.add("张三_"+id);
        rowData.add("备注xxx"+"_"+id);
        rowData.add("53.12");
        rowData.add("31215.00");
        rowData.add("1980-01-05");
        rowData.add("1980-01-05 13:15:16.123456");
        rowData.add("12:15:13");
        return rowData;
    }

    /**
     * 生僻字用转义unicode
     * @param id
     * @return
     */
    private List<String> generateRowDataUsingUnicode(Long id) {
        List<String> rowData = new ArrayList<>();
        rowData.add(id.toString());
        rowData.add("张\\00009F98\\\\6724\\_"+id);
        rowData.add("备注xxxC:\\Windows\\"+"_"+id);
        rowData.add("51.12");
        rowData.add("31111.00");
        rowData.add("1981-01-05");
        rowData.add("1981-01-05 13:15:16.123456");
        rowData.add("11:15:13");
        return rowData;
    }

    @Test
    @DisplayName("源ibm1388 csv文件有错误编码")
    void testExecute_WithErrorEncode() throws IOException {
        testExecute_WithErrorEncode(10, "\n");
    }

    @Test
    @DisplayName("超过最大允许错误, 源ibm1388 csv文件有错误编码")
    void testExecute_WithErrorEncodeMoreThatLimit() throws IOException {
        testExecute_WithErrorEncode(3, "\n");
    }

    @Test
    @DisplayName("源ibm1388 csv文件有错误编码, 换行符")
    void testExecute_WithErrorEncode_IbmCsvLineSeparator() throws IOException {
        testExecute_WithErrorEncode(10, "\n\n");
    }

    @Test
    @DisplayName("超过最大允许错误, 源ibm1388 csv文件有错误编码, 换行符")
    void testExecute_WithErrorEncodeMoreThatLimit_IbmCsvLineSeparator() throws IOException {
        testExecute_WithErrorEncode(3, "\n\n");
    }


    void testExecute_WithErrorEncode(int maxErrorCount, String ibmCsvLineSeparator) throws IOException {
        // --- 1. 动态准备数据 ---
        Long jobId = 100L;
        Long qianyiId = 10L;
        Long detailId = 1L;

        Path sourceFile = tempDir.resolve("source_ibm.csv");

        // 2. 准备编码
        // 正常行：使用 IBM-1388 (EBCDIC编码)
        Charset validCharset = Charset.forName("IBM-1388");
        // 错误行：使用 UTF-8 (对于 IBM-1388 解码器来说，UTF-8 的字节序列通常是非法的)
        Charset badCharset = StandardCharsets.UTF_8;

        // 写入数据行
        int totalDataRows = 20;
        List<Integer> errorCodeLines = List.of(4,7,9,10);
        boolean errorMoreThatLimit = errorCodeLines.size() >= maxErrorCount;

        List<List<String>> rows = new ArrayList<>();
        try (FileOutputStream fos = new FileOutputStream(sourceFile.toString())) {
            for (int i = 1; i <= totalDataRows; i++) {
                List<String> row = generateRowData(i + 115L);
                rows.add(row);
                StringBuilder csvContent = new StringBuilder();
                csvContent.append(row.stream().collect(Collectors.joining(",")));
                if (errorCodeLines.contains(i)) {
                    // 【制造坏数据】
                    // 我们写入一段包含中文的 UTF-8 字节流。
                    // IBM-1388 解析器读到这些字节时，因为没有 Shift-Out/Shift-In 标识，
                    // 或者字节值不符合 EBCDIC 规范，就会报错或乱码。
                    fos.write(csvContent.toString().getBytes(badCharset));
                } else {
                    // 【写入正常数据】
                    // 正常的
                    fos.write(csvContent.toString().getBytes(validCharset));
                }
                fos.write(ibmCsvLineSeparator.getBytes(validCharset));
            }
        }

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

        mockAppConfig("IBM1388", ibmCsvLineSeparator, maxErrorCount, false);

        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(anyString()))
                .thenReturn(List.of("id,name,remark,weight,salary,birth,birthday,lunchtime".split(",")));

        transcodeService.execute(detailId);

        Path errorFile = Paths.get(MigrationOutputDirectorUtil.transcodeErrorFile(job, qianyiId, detailId));
        assertTrue(Files.exists(errorFile), "转码失败文件存在");

        Path expectedSplitFile = Paths.get(MigrationOutputDirectorUtil.transcodeSplitFile(job, qianyiId, detailId, 1));
        assertTrue(Files.exists(expectedSplitFile), "拆分文件存在");
        List<String> fileContents = Files.readAllLines(expectedSplitFile, StandardCharsets.UTF_8);
        if (!errorMoreThatLimit) {
            assertEquals(totalDataRows - errorCodeLines.size(), fileContents.size());
        }

        for(int irow=0; irow < fileContents.size(); irow++) {
            Pair<List<String>, Integer> listIntegerPair = parseUtf8Row(fileContents.get(irow));
            List<String> row = rows.get(listIntegerPair.getValue()-1);
            assertIbm1388AndUtf8RowEquals(row, listIntegerPair);
        }

        if (!errorMoreThatLimit) {
            // 验证数据库保存次数 (总的 save 调用次数应该等于切片文件数)
            verify(splitRepo).save(any(CsvSplit.class));
            // 验证处理总状态
            verify(stateManager).updateDetailStatus(eq(detailId), eq(DetailStatus.PROCESSING_CHILDS));

            verify(detailRepo).updateSourceRowCount(eq(detailId), eq(1L*totalDataRows));
            verify(detailRepo).updateErrorCount(eq(detailId), eq(1L*errorCodeLines.size()));
        } else {
            verify(stateManager).updateDetailStatus(eq(detailId), eq(DetailStatus.FAIL_TRANSCODE));
        }
    }

    @Test
    @DisplayName("源ibm1388 csv含有不合法字符被替换成\uFFFD")
    void testExecute_WithErrorEncode_1() throws IOException {
        Long jobId = 100L;
        Long qianyiId = 10L;
        Long detailId = 1L;

        Path sourceFile = tempDir.resolve("source_ibm.csv");
        Charset validCharset = Charset.forName("IBM-1388");
        try (FileOutputStream fos = new FileOutputStream(sourceFile.toString())) {
            {
                fos.write("1,".getBytes(validCharset));
                fos.write("张三_1".getBytes(validCharset)); fos.write(",".getBytes(validCharset));
                fos.write("备注xxx".getBytes(validCharset)); fos.write("\n".getBytes(validCharset));
            }
            {
                fos.write("2,".getBytes(validCharset));
                // 0x0E (Shift-Out)：切换到双字节模式 (DBCS)。
                // 0x0F (Shift-In)：切换回单字节模式 (SBCS)。
                fos.write(new byte[]{(byte)0x0E, 0x45, 0x00, (byte)0x0F}); fos.write(",".getBytes(validCharset));
                fos.write("备注xxx".getBytes(validCharset));
            }
        }

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

        mockAppConfig("IBM1388", "\n", 100, false);

        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(anyString()))
                .thenReturn(List.of("id,name,remark".split(",")));

        transcodeService.execute(detailId);

        Path errorFile = Paths.get(MigrationOutputDirectorUtil.transcodeErrorFile(job, qianyiId, detailId));
        assertTrue(Files.exists(errorFile), "转码失败文件存在");
        List<String> errorFileContents = Files.readAllLines(errorFile, StandardCharsets.UTF_8);
        assertEquals(2, errorFileContents.size());
        assertTrue(errorFileContents.get(1).contains("\"[{\"\"columnIndex\"\":1,\"\"errorReason\"\":\"\"CONTAINS_REPLACEMENT_CHAR\"\""));

        Path expectedSplitFile = Paths.get(MigrationOutputDirectorUtil.transcodeSplitFile(job, qianyiId, detailId, 1));
        assertTrue(Files.exists(expectedSplitFile), "拆分文件存在");
        List<String> fileContents = Files.readAllLines(expectedSplitFile, StandardCharsets.UTF_8);
        assertEquals(1, fileContents.size());

        verify(splitRepo).save(any(CsvSplit.class));
        verify(stateManager).updateDetailStatus(eq(detailId), eq(DetailStatus.PROCESSING_CHILDS));

        verify(detailRepo).updateSourceRowCount(eq(detailId), eq(2L));
        verify(detailRepo).updateErrorCount(eq(detailId), eq(1L));
    }

    @Test
    @DisplayName("打开tunneling开关, ibm1388不能表示的中文生僻字，用unicode转义表示")
    void testExecute_Tunneling() throws IOException {
        testExecute_Tunneling(true);
    }

    @Test
    @DisplayName("关闭tunneling开关, ibm1388不能表示的中文生僻字，用unicode转义表示")
    void testExecute_closeTunneling() throws IOException {
        testExecute_Tunneling(false);
    }

    void testExecute_Tunneling(boolean tunneling) throws IOException {
        // --- 1. 动态准备数据 ---
        Long jobId = 100L;
        Long qianyiId = 10L;
        Long detailId = 1L;

        Path sourceFile = tempDir.resolve("source_ibm.csv");

        // 2. 准备编码
        // 正常行：使用 IBM-1388 (EBCDIC编码)
        Charset validCharset = Charset.forName("IBM-1388");

        // 写入数据行
        int totalDataRows = 3;
        List<List<String>> rows = new ArrayList<>();
        try (FileOutputStream fos = new FileOutputStream(sourceFile.toString())) {
            {
                List<String> row = generateRowData(1L);
                rows.add(row);
                StringBuilder csvContent = new StringBuilder();
                csvContent.append(row.stream().collect(Collectors.joining(",")));
                fos.write(csvContent.toString().getBytes(validCharset));
                fos.write("\n\n".getBytes(validCharset));
            }

            {
                List<String> row = generateRowDataUsingUnicode(2L);
                rows.add(row);
                StringBuilder csvContent = new StringBuilder();
                csvContent.append(row.stream().collect(Collectors.joining(",")));
                fos.write(csvContent.toString().getBytes(validCharset));
                fos.write("\n\n".getBytes(validCharset));
            }

            {
                List<String> row = generateRowData(3L);
                rows.add(row);
                StringBuilder csvContent = new StringBuilder();
                csvContent.append(row.stream().collect(Collectors.joining(",")));
                fos.write(csvContent.toString().getBytes(validCharset));
                fos.write("\n\n".getBytes(validCharset));
            }
        }

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

        mockAppConfig("IBM1388", "\n\n", 10, tunneling);

        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(anyString()))
                .thenReturn(List.of("id,name,remark,weight,salary,birth,birthday,lunchtime".split(",")));

        transcodeService.execute(detailId);

        Path errorFile = Paths.get(MigrationOutputDirectorUtil.transcodeErrorFile(job, qianyiId, detailId));
        assertFalse(Files.exists(errorFile), "转码失败文件应该存在");

        Path expectedSplitFile = Paths.get(MigrationOutputDirectorUtil.transcodeSplitFile(job, qianyiId, detailId, 1));
        assertTrue(Files.exists(expectedSplitFile), "拆分文件存在");
        List<String> fileContents = Files.readAllLines(expectedSplitFile, StandardCharsets.UTF_8);
        assertEquals(totalDataRows, fileContents.size());

        {
            Pair<List<String>, Integer> listIntegerPair = parseUtf8Row(fileContents.get(0));
            List<String> row = rows.get(listIntegerPair.getValue() - 1);
            assertIbm1388AndUtf8RowEquals(row, listIntegerPair);
        }

        if (tunneling) {
            // unicode转义
            Pair<List<String>, Integer> listIntegerPair = parseUtf8Row(fileContents.get(1));
            List<String> row = rows.get(listIntegerPair.getValue() - 1);
            for(int j=0; j<row.size(); j++) {
                if (j==1) {
                    assertEquals("张龘朤_2", listIntegerPair.getKey().get(1)); // name
                } else {
                    assertEquals(row.get(j), listIntegerPair.getKey().get(j));
                }
            }
        } else {
            Pair<List<String>, Integer> listIntegerPair = parseUtf8Row(fileContents.get(1));
            List<String> row = rows.get(listIntegerPair.getValue() - 1);
            assertIbm1388AndUtf8RowEquals(row, listIntegerPair);
        }

        {
            Pair<List<String>, Integer> listIntegerPair = parseUtf8Row(fileContents.get(2));
            List<String> row = rows.get(listIntegerPair.getValue() - 1);
            assertIbm1388AndUtf8RowEquals(row, listIntegerPair);
        }

        // 验证数据库保存次数 (总的 save 调用次数应该等于切片文件数)
        verify(splitRepo).save(any(CsvSplit.class));
        // 验证处理总状态
        verify(stateManager).updateDetailStatus(eq(detailId), eq(DetailStatus.PROCESSING_CHILDS));

        verify(detailRepo).updateSourceRowCount(eq(detailId), eq(1L * totalDataRows));
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

        mockAppConfig("IBM1388", "\n", 50, false);

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
    private void mockAppConfig(
            String ibmCsvEncoding, String ibmCsvLineSeparator, int maxErrorCount, boolean tunneling) {
        // 创建配置层级
        AppProperties.CsvDetailConfig ibmConfig = new AppProperties.CsvDetailConfig();
        ibmConfig.setEncoding(ibmCsvEncoding);
        ibmConfig.setTunneling(tunneling);
        ibmConfig.setLineSeparator(ibmCsvLineSeparator);

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

}