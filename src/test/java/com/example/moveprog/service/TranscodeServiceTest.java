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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
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
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeSplitDirectory(anyString()))
                .thenReturn(splitDir.toString());
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeErrorDirectory(anyString()))
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
        verify(stateManager).updateDetailStatus(detailId, DetailStatus.TRANSCODING);
        verify(stateManager).updateDetailStatus(detailId, DetailStatus.PROCESSING_CHILDS);

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
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.verifyResultDirectory(anyString()))
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
            verify(targetDatabaseConnectionManager).deleteLoadOldData(any(), eq(detailId), eq(555L));
        } catch (Exception e) {
            fail("Exception verifying db call");
        }
    }

    /**
     * 测试任务中断异常处理
     */
    @Test
    void testExecute_JobStopped() {
        Long detailId = 1L;
        // 模拟清理阶段抛出“任务停止”异常
        when(detailRepo.findById(detailId)).thenThrow(new JobStoppedException("Stop by user"));

        transcodeService.execute(detailId);

        // 验证没有更新为 FAIL_TRANSCODE (中断是正常的停止流程)
        verify(stateManager, never()).updateDetailStatus(eq(detailId), eq(DetailStatus.FAIL_TRANSCODE));
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

        AppProperties.TranscodeJob jobConfig = new AppProperties.TranscodeJob();
        jobConfig.setMaxErrorCount(10);

        // Mock config 行为
        lenient().when(config.getCsv()).thenReturn(csv);
        lenient().when(config.getPerformance()).thenReturn(perf);
        lenient().when(config.getTranscodeJob()).thenReturn(jobConfig);
    }
}