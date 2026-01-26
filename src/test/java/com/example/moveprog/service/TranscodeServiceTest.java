package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.DetailStatus;
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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

    // 使用 JUnit 5 的临时目录功能，测试完自动清理
    @TempDir
    Path tempDir;

    private MockedStatic<SchemaParseUtil> schemaParseUtilMock;
    private MockedStatic<MigrationOutputDirectorUtil> outputUtilMock;
    private MockedStatic<CharsetFactory> charsetFactoryMock;
    private MockedStatic<FastEscapeHandler> fastEscapeHandlerMock;

    @BeforeEach
    void setUp() {
        // Mock 静态工具类
        schemaParseUtilMock = Mockito.mockStatic(SchemaParseUtil.class);
        outputUtilMock = Mockito.mockStatic(MigrationOutputDirectorUtil.class);
        charsetFactoryMock = Mockito.mockStatic(CharsetFactory.class);
        fastEscapeHandlerMock = Mockito.mockStatic(FastEscapeHandler.class);
    }

    @AfterEach
    void tearDown() {
        // 释放静态 Mock
        schemaParseUtilMock.close();
        outputUtilMock.close();
        charsetFactoryMock.close();
        fastEscapeHandlerMock.close();
    }

    @Test
    void testExecute_HappyPath() throws IOException {
        // --- 1. 准备测试数据和环境 ---
        Long detailId = 100L;
        Long qianyiId = 200L;
        Long jobId = 300L;
        String mockEncoding = "UTF-8"; // 测试用 UTF-8 模拟 IBM 编码，简化 byte 操作

        // 创建临时的源文件 (模拟 IBM1388 csv)
        Path sourceFile = tempDir.resolve("source.csv");
        String csvContent = "\"col1\",\"col2\"\n\"val1\",\"val2\"\n\"val3\",\"val4\"";
        Files.writeString(sourceFile, csvContent, StandardCharsets.UTF_8);

        // 创建输出目录
        Path outDir = tempDir.resolve("out");
        Path splitDir = outDir.resolve("split");
        Path errorDir = outDir.resolve("error");
        Files.createDirectories(splitDir);
        Files.createDirectories(errorDir);

        // --- 2. Mock 实体对象 ---
        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setQianyiId(qianyiId);
        detail.setJobId(jobId);
        detail.setStatus(DetailStatus.TRANSCODING);
        detail.setSourceCsvPath(sourceFile.toString());

        Qianyi qianyi = new Qianyi();
        qianyi.setId(qianyiId);
        qianyi.setJobId(jobId);
        qianyi.setDdlFilePath("dummy.sql");

        MigrationJob job = new MigrationJob();
        job.setId(jobId);
        job.setOutDirectory(outDir.toString());

        // --- 3. Mock 仓库行为 ---
        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));
        when(qianyiRepo.findById(qianyiId)).thenReturn(Optional.of(qianyi));
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));
        
        // Mock 静态工具类行为
        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(any())).thenReturn(Collections.emptyList()); // 假设没有DDL校验或返回空
        
        // Mock 路径生成逻辑，指向我们的临时目录
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeSplitDirectory(any())).thenReturn(splitDir.toString());
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeErrorDirectory(any())).thenReturn(errorDir.toString());
        // 模拟生成切片文件名: splitDir/100_1.csv
        Path splitFile = splitDir.resolve(detailId + "_1.csv");
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeSplitFile(any(), eq(detailId), eq(1)))
                      .thenReturn(splitFile.toString());

        // Mock 字符集工厂
        charsetFactoryMock.when(() -> CharsetFactory.resolveCharset(any())).thenReturn(StandardCharsets.UTF_8);
        charsetFactoryMock.when(() -> CharsetFactory.createDecoder(any(), anyBoolean()))
                          .thenCallRealMethod(); // 如果 createDecoder 逻辑简单可以直接调用真实，或者 mock 返回 UTF8 decoder

        // --- 4. Mock AppProperties 配置 (最繁琐的一步) ---
        mockAppConfig(mockEncoding);

        // --- 5. 执行测试 ---
        transcodeService.execute(detailId);

        // --- 6. 验证结果 ---
        // 验证状态更新流程
        verify(stateManager).updateDetailStatus(detailId, DetailStatus.TRANSCODING);
        verify(stateManager).updateDetailStatus(detailId, DetailStatus.PROCESSING_CHILDS);

        // 验证切片文件是否生成
        assertTrue(Files.exists(splitFile), "切片文件应该被创建");
        
        // 验证切片内容 (注意：代码逻辑中会追加一列行号)
        String outputContent = Files.readString(splitFile, StandardCharsets.UTF_8);
        // 原文: "val1","val2" -> 预期: "val1","val2","2" (假设 context.currentLine() 是 2，因为第一行是header被读过了? 或者 parseNext 逻辑)
        // Univocity parser behavior: parseNext() gets row. 
        // 你的代码逻辑: newRow[last] = currentLineNoFromContext
        // 检查是否包含原内容
        assertTrue(outputContent.contains("\"val1\",\"val2\""));
        assertTrue(outputContent.contains("\"val3\",\"val4\""));

        // 验证是否保存了 Split 记录
        verify(splitRepo, atLeastOnce()).save(any());
        
        // 验证错误计数更新
        verify(detailRepo).updateErrorCount(eq(detailId), eq(0L));
    }

    @Test
    void testExecute_ExceptionHandling() {
        Long detailId = 100L;
        // 模拟 repository 抛出异常
        when(detailRepo.findById(detailId)).thenThrow(new RuntimeException("DB Connection Failed"));

        transcodeService.execute(detailId);

        // 验证状态更新为 FAIL_TRANSCODE
        verify(stateManager).updateDetailStatus(detailId, DetailStatus.FAIL_TRANSCODE);
    }

    // --- 辅助方法：构建复杂的 AppProperties ---
    private void mockAppConfig(String encoding) {
        // 创建各级配置对象
        AppProperties.Csv csv = new AppProperties.Csv();
        AppProperties.CsvDetailConfig ibmSource = new AppProperties.CsvDetailConfig();
        ibmSource.setEncoding(encoding);
        ibmSource.setTunneling(false); // 关闭 Tunneling 简化测试
        // 设置 ParserSettings 默认值
        // 注意：如果 CsvDetailConfig 内部字段有默认值最好，否则这里要手动 set
        
        AppProperties.CsvDetailConfig utf8Split = new AppProperties.CsvDetailConfig();
        utf8Split.setEncoding("UTF-8");

        csv.setIbmSource(ibmSource);
        csv.setUtf8Split(utf8Split);

        AppProperties.Performance perf = new AppProperties.Performance();
        perf.setReadBufferSize(1024);
        perf.setWriteBufferSize(1024);
        perf.setSplitRows(1000); // 设置大一点，让它只生成一个文件

        AppProperties.TranscodeJob transcodeJob = new AppProperties.TranscodeJob();
        transcodeJob.setMaxErrorCount(10);

        // 组装
        when(config.getCsv()).thenReturn(csv);
        when(config.getPerformance()).thenReturn(perf);
        when(config.getTranscodeJob()).thenReturn(transcodeJob);
    }
}