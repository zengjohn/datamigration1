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
import com.example.moveprog.util.CharsetFactory;
import com.example.moveprog.util.FastEscapeHandler;
import com.example.moveprog.util.MigrationOutputDirectorUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TranscodeServiceTest {

    @Mock private StateManager stateManager;
    @Mock private MigrationJobRepository jobRepository;
    @Mock private QianyiRepository qianyiRepo;
    @Mock private QianyiDetailRepository detailRepo;
    @Mock private CsvSplitRepository splitRepo;
    @Mock private JobControlManager jobControlManager;
    @Mock private TargetDatabaseConnectionManager targetDatabaseConnectionManager;
    @Mock private AppProperties config;

    @InjectMocks
    private TranscodeService transcodeService;

    // JUnit 5 临时目录，测试结束后自动删除
    @TempDir
    Path tempDir;

    // 静态方法的 Mock 对象
    private MockedStatic<SchemaParseUtil> schemaParseUtilMock;
    private MockedStatic<MigrationOutputDirectorUtil> outputUtilMock;
    private MockedStatic<CharsetFactory> charsetFactoryMock;
    private MockedStatic<FastEscapeHandler> fastEscapeHandlerMock;

    @BeforeEach
    void setUp() {
        schemaParseUtilMock = Mockito.mockStatic(SchemaParseUtil.class);
        outputUtilMock = Mockito.mockStatic(MigrationOutputDirectorUtil.class);
        charsetFactoryMock = Mockito.mockStatic(CharsetFactory.class);
        fastEscapeHandlerMock = Mockito.mockStatic(FastEscapeHandler.class);
    }

    @AfterEach
    void tearDown() {
        schemaParseUtilMock.close();
        outputUtilMock.close();
        charsetFactoryMock.close();
        fastEscapeHandlerMock.close();
    }

    /**
     * 测试核心流程：正常转码、切分文件、保存记录
     */
    @Test
    void testExecute_HappyPath() throws IOException {
        // --- 1. 准备数据 ---
        Long detailId = 1L;
        Long qianyiId = 10L;
        Long jobId = 100L;

        // 模拟源文件 (内容假设是 CSV 格式)
        Path sourceFile = tempDir.resolve("source_ibm.csv");
        // 这里写入 UTF-8，后续通过 Mock 让 CharsetFactory 认为它是 IBM-1388 从而能读出来
        String csvContent = "\"name\",\"age\"\n\"张三\",\"18\"\n\"李四\",\"20\"";
        Files.writeString(sourceFile, csvContent, StandardCharsets.UTF_8);

        // 模拟输出目录结构 (outDirectory 现在来自 MigrationJob)
        Path outDir = tempDir.resolve("output_home");
        Path splitDir = outDir.resolve("split_dir");
        Path errorDir = outDir.resolve("error_dir");
        Files.createDirectories(splitDir);
        Files.createDirectories(errorDir);

        // --- 2. Mock 实体 ---
        MigrationJob job = new MigrationJob();
        job.setId(jobId);
        job.setOutDirectory(outDir.toString()); // 【变更点】从 Job 获取目录

        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setQianyiId(qianyiId);
        detail.setJobId(jobId);
        detail.setSourceCsvPath(sourceFile.toString());
        detail.setStatus(DetailStatus.TRANSCODING); // 初始状态

        Qianyi qianyi = new Qianyi();
        qianyi.setId(qianyiId);
        qianyi.setJobId(jobId);
        qianyi.setDdlFilePath("schema.sql"); // 假路径

        // --- 3. Mock Repository ---
        // 注意：代码中有两次 findById(detailId)，一次是清理旧数据，一次是双重检查
        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));
        when(qianyiRepo.findById(qianyiId)).thenReturn(Optional.of(qianyi));

        // --- 4. Mock AppProperties ---
        mockAppConfig("IBM-1388"); // 传入配置编码

        // --- 5. Mock 静态工具类行为 ---
        // 5.1 模拟 DDL 解析 (返回空列表表示不校验列数，或者 mock 具体列)
        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(anyString()))
                .thenReturn(Collections.emptyList());

        // 5.2 模拟路径生成 (全部指向 tempDir)
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeSplitDirectory(job, qianyiId))
                .thenReturn(splitDir.toString());
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeErrorDirectory(job, qianyiId))
                .thenReturn(errorDir.toString());

        // 模拟生成的切片文件路径: .../split_dir/1_1.csv
        Path expectedSplitFile = splitDir.resolve(detailId + "_1.csv");
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeSplitFile(anyString(), eq(detailId), eq(1)))
                .thenReturn(expectedSplitFile.toString());

        // 5.3 模拟 Charset (关键技巧：把 IBM-1388 映射回 UTF-8，这样我们可以读普通文件)
        Charset utf8 = StandardCharsets.UTF_8;
        charsetFactoryMock.when(() -> CharsetFactory.resolveCharset(anyString())).thenReturn(utf8);
        // createDecoder 比较复杂，建议让它调用真实逻辑，因为我们已经把 Charset 替换成了 UTF-8
        charsetFactoryMock.when(() -> CharsetFactory.createDecoder(any(Charset.class), anyBoolean()))
                .thenCallRealMethod();

        // --- 6. 执行 ---
        transcodeService.execute(detailId);

        // --- 7. 验证 ---
        // 7.1 验证状态流转
        verify(stateManager).updateDetailStatus(detailId, DetailStatus.PROCESSING_CHILDS);
        verify(splitRepo, atLeastOnce()).save(any(CsvSplit.class));

        // 7.2 验证切片文件生成
        assertTrue(Files.exists(expectedSplitFile), "切片文件应该被创建");
        String fileContent = Files.readString(expectedSplitFile);
        // 验证内容是否被转码并写入 (你的代码会加一列行号)
        // 原始内容: "张三","18" -> 期望包含: "张三","18"
        assertTrue(fileContent.contains("张三"), "切片文件应包含数据");

        // 7.3 验证数据库保存
        // 验证是否保存了 split 记录
        verify(splitRepo, atLeastOnce()).save(any(CsvSplit.class));
        // 验证是否更新了错误行数
        verify(detailRepo).updateErrorCount(eq(detailId), eq(0L));
    }

    /**
     * 测试清理旧数据逻辑
     */
    @Test
    void testCleanUpOldSplits() throws IOException {
        Long detailId = 1L;
        Long jobId = 100L;

        // 准备旧的切片文件
        Path splitFile = tempDir.resolve("old_split.csv");
        Files.writeString(splitFile, "data");

        // Mock 实体
        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setJobId(jobId);

        MigrationJob job = new MigrationJob();
        job.setId(jobId);
        job.setOutDirectory(tempDir.toString()); // 【变更点】

        CsvSplit oldSplit = new CsvSplit();
        oldSplit.setId(555L);
        oldSplit.setSplitFilePath(splitFile.toString()); // 设置真实存在的路径以便测试删除

        // Mock Repo
        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));
        when(splitRepo.findByDetailId(detailId)).thenReturn(List.of(oldSplit));

        // Mock OutputUtil 以便计算 verifyResultFile 路径
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.verifyResultDirectory(job, anyLong()))
                .thenReturn(tempDir.resolve("verify").toString());
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.verifyResultFile(anyString(), eq(555L)))
                .thenReturn(tempDir.resolve("verify/diff_555.txt").toString());

        // 执行清理
        transcodeService.cleanUpOldSplits(detailId);

        // 验证
        assertFalse(Files.exists(splitFile), "旧的切片文件应该被物理删除");
        verify(splitRepo).deleteByDetailId(detailId); // 验证 DB 删除调用
        // 验证是否调用了目标库清理
        try {
            verify(targetDatabaseConnectionManager).deleteLoadOldData(eq(555L));
        } catch (Exception e) {
            fail("Exception verifying db call");
        }
    }

    /**
     * 测试任务中断异常处理
     */
    @Test
    void testExecute_JobStopped() throws IOException {
        Long detailId = 1L;
        Long jobId = 100L;

        Path sourceFile = tempDir.resolve("source.csv");
        Files.createFile(sourceFile);

        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setJobId(jobId);
        detail.setSourceCsvPath(sourceFile.toString());
        detail.setStatus(DetailStatus.TRANSCODING);

        // 1. findById 必须成功，才能进入 try 块
        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));

        // 2. 在 try 块内部抛出 JobStoppedException
        // cleanUpOldSplits -> jobRepository.findById，我们在这里埋雷
        when(jobRepository.findById(jobId)).thenThrow(new JobStoppedException("Stop by user"));

        transcodeService.execute(detailId);

        // 验证：异常被捕获，没有抛出到外面，也没有更新为 FAIL
        verify(stateManager, never()).updateDetailStatus(eq(detailId), eq(DetailStatus.FAIL_TRANSCODE));
    }

    /**
     * 【新】测试拆分逻辑
     * 场景：5行数据，SplitRows=3
     * 预期：产生2个 Split 文件 (3行 + 2行)
     * 目的：复现 "Off-by-one" 或 逻辑 Bug
     */
    @Test
    void testExecute_WithSplitting() throws IOException {
        Long detailId = 1L;
        Long jobId = 100L;

        // 1. 构造 5 行数据 (带表头实际是 6 行文本，Unvocity 解析出 5 条记录)
        // Header
        // Row 1 (Line 2)
        // Row 2 (Line 3)
        // Row 3 (Line 4)
        // Row 4 (Line 5)
        // Row 5 (Line 6)
        Path sourceFile = tempDir.resolve("source_split.csv");
        StringBuilder sb = new StringBuilder();
        //sb.append("\"col1\"\n");
        for (int i = 1; i <= 5; i++) {
            sb.append("\"val").append(i).append("\"\n");
        }
        Files.writeString(sourceFile, sb.toString(), StandardCharsets.UTF_8);

        Path outDir = tempDir.resolve("out_split");
        Path splitDir = outDir.resolve("splits");
        Path errorDir = outDir.resolve("errors");
        Files.createDirectories(splitDir);
        Files.createDirectories(errorDir);

        setupCommonMocks(detailId, jobId, sourceFile, outDir, splitDir, errorDir);

        // 2. 覆盖配置：设置拆分行数为 3
        AppProperties.Performance perf = new AppProperties.Performance();
        perf.setReadBufferSize(1024);
        perf.setWriteBufferSize(1024);
        perf.setSplitRows(3); // 【关键】设置每 3 行拆分
        when(config.getPerformance()).thenReturn(perf);

        // Mock 切片文件名生成逻辑 (支持多次调用)
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeSplitFile(anyString(), eq(detailId), anyInt()))
                .thenAnswer(inv -> splitDir.resolve(detailId + "_" + inv.getArgument(2) + ".csv").toString());

        // 3. 执行
        transcodeService.execute(detailId);

        // 4. 捕获所有保存的 Split
        ArgumentCaptor<CsvSplit> splitCaptor = ArgumentCaptor.forClass(CsvSplit.class);
        verify(splitRepo, atLeast(1)).save(splitCaptor.capture());
        List<CsvSplit> savedSplits = splitCaptor.getAllValues();

        // 5. 断言验证 (如果代码有 Bug，这里会挂)
        System.out.println("Saved Splits: " + savedSplits.size());
        savedSplits.forEach(s -> System.out.println("Split: Start=" + s.getStartRowNo() + ", Count=" + s.getRowCount()));

        // 期望：2个切片
        // Split 1: 3行 (Row1,2,3)
        // Split 2: 2行 (Row4,5)
        assertEquals(2, savedSplits.size(), "应该生成 2 个切片记录");

        CsvSplit split1 = savedSplits.get(0);
        assertEquals(3, split1.getRowCount(), "第1个切片应包含 3 行");

        CsvSplit split2 = savedSplits.get(1);
        assertEquals(2, split2.getRowCount(), "第2个切片应包含 2 行");
    }

    private void setupCommonMocks(Long detailId, Long jobId, Path sourceFile, Path outDir, Path splitDir, Path errorDir) {
        MigrationJob job = new MigrationJob();
        job.setId(jobId);
        job.setOutDirectory(outDir.toString());

        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setQianyiId(10L);
        detail.setJobId(jobId);
        detail.setSourceCsvPath(sourceFile.toString());
        detail.setStatus(DetailStatus.TRANSCODING);

        Qianyi qianyi = new Qianyi();
        qianyi.setId(10L);
        qianyi.setJobId(jobId);
        qianyi.setDdlFilePath("schema.sql");

        lenient().when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));
        lenient().when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));
        lenient().when(qianyiRepo.findById(10L)).thenReturn(Optional.of(qianyi));

        mockAppConfig("IBM-1388");

        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(anyString())).thenReturn(Collections.emptyList());
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeSplitDirectory(job, anyLong())).thenReturn(splitDir.toString());
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeErrorDirectory(job, anyLong())).thenReturn(errorDir.toString());
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeSplitFile(anyString(), eq(detailId), eq(1)))
                .thenReturn(splitDir.resolve(detailId + "_1.csv").toString());

        Charset utf8 = StandardCharsets.UTF_8;
        charsetFactoryMock.when(() -> CharsetFactory.resolveCharset(anyString())).thenReturn(utf8);
        charsetFactoryMock.when(() -> CharsetFactory.createDecoder(any(Charset.class), anyBoolean())).thenCallRealMethod();
    }

    // --- 辅助：组装 AppProperties ---
    private void mockAppConfig(String encoding) {
        // 创建配置层级
        AppProperties.CsvDetailConfig ibmConfig = new AppProperties.CsvDetailConfig();
        ibmConfig.setEncoding(encoding);
        ibmConfig.setTunneling(false);
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
        jobConfig.setMaxErrorCount(10);

        // Mock config 行为
        lenient().when(config.getCsv()).thenReturn(csv);
        lenient().when(config.getPerformance()).thenReturn(perf);
        lenient().when(config.getTranscode()).thenReturn(jobConfig);
    }
}