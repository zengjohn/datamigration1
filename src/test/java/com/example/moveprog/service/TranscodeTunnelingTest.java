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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TranscodeTunnelingTest {

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

    @TempDir
    Path tempDir;

    private MockedStatic<SchemaParseUtil> schemaParseUtilMock;
    private MockedStatic<MigrationOutputDirectorUtil> outputUtilMock;
    private MockedStatic<CharsetFactory> charsetFactoryMock;
    //private MockedStatic<FastEscapeHandler> fastEscapeHandlerMock;

    @BeforeEach
    void setUp() {
        schemaParseUtilMock = Mockito.mockStatic(SchemaParseUtil.class);
        outputUtilMock = Mockito.mockStatic(MigrationOutputDirectorUtil.class);
        charsetFactoryMock = Mockito.mockStatic(CharsetFactory.class);
        //fastEscapeHandlerMock = Mockito.mockStatic(FastEscapeHandler.class);
    }

    @AfterEach
    void tearDown() {
        schemaParseUtilMock.close();
        outputUtilMock.close();
        charsetFactoryMock.close();
        //fastEscapeHandlerMock.close();
    }

    /**
     * 测试场景 1：开启 Tunneling (生僻字还原)
     * 输入: "张\2CC56\三"
     * 预期输出: "张𬱖三" (生僻字被还原)
     */
    @Test
    void testTunneling_Enabled() throws IOException {
        // 1. 准备含转义符的测试数据
        // 注意：在 Java 字符串中，反斜杠本身需要转义，所以 "\\2CC56\\" 代表文件里的 "\2CC56\"
        String rawContent = "\"id\",\"name\"\n\"1\",\"张\\2CC56\\三\"";
        
        // 2. 执行测试，开启 tunneling = true
        String outputContent = executeTranscodeTest(rawContent, true);

        // 3. 验证结果
        // 预期：\2CC56\ 被还原为 Unicode 字符 U+2CC56 (𬱖)
        // 𬱖 是一个超出 BMP 的生僻字 (Surrogate Pair)
        String expectedChar = new String(Character.toChars(0x2CC56)); 
        
        assertTrue(outputContent.contains("张" + expectedChar + "三"), 
                "开启 Tunneling 时，转义符应被还原为真实汉字 (张𬱖三)");
        assertFalse(outputContent.contains("\\2CC56\\"), 
                "开启 Tunneling 时，原始的转义序列不应存在");
    }

    /**
     * 测试场景 2：关闭 Tunneling (原样传输)
     * 输入: "张\2CC56\三"
     * 预期输出: "张\2CC56\三" (保留原样)
     */
    @Test
    void testTunneling_Disabled() throws IOException {
        // 1. 准备相同的测试数据
        String rawContent = "\"id\",\"name\"\n\"1\",\"张\\2CC56\\三\"";

        // 2. 执行测试，开启 tunneling = false
        String outputContent = executeTranscodeTest(rawContent, false);

        // 3. 验证结果
        // 预期：内容原封不动，反斜杠被保留（注意 CSVWriter 可能会对含反斜杠的字段加引号，这是正常的）
        // 只要能找到 \2CC56\ 序列即可
        assertTrue(outputContent.contains("\\2CC56\\"), 
                "关闭 Tunneling 时，应保留原始转义序列");
        
        // 确保没有误把生僻字还原出来
        String expectedChar = new String(Character.toChars(0x2CC56));
        assertFalse(outputContent.contains(expectedChar), 
                "关闭 Tunneling 时，不应执行转义还原");
    }

    // --- 核心测试执行逻辑封装 ---
    private String executeTranscodeTest(String csvContent, boolean enableTunneling) throws IOException {
        Long detailId = 1L;
        Long jobId = 100L;
        Long qianyiId = 200L;

        // 1. 写入源文件
        Path sourceFile = tempDir.resolve("source_tunnel.csv");
        Files.writeString(sourceFile, csvContent, StandardCharsets.UTF_8);

        // 2. 模拟目录
        Path outDir = tempDir.resolve("out");
        Path splitDir = outDir.resolve("split");
        Path errorDir = outDir.resolve("error");
        Files.createDirectories(splitDir);
        Files.createDirectories(errorDir);

        // 3. Mock 路径生成
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeSplitDirectory(any())).thenReturn(splitDir.toString());
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeErrorDirectory(any())).thenReturn(errorDir.toString());
        Path splitFile = splitDir.resolve("result.csv");
        outputUtilMock.when(() -> MigrationOutputDirectorUtil.transcodeSplitFile(any(), any(), anyInt()))
                      .thenReturn(splitFile.toString());

        // 4. Mock 实体
        MigrationJob job = new MigrationJob();
        job.setId(jobId);
        job.setOutDirectory(outDir.toString());

        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setQianyiId(qianyiId);
        detail.setJobId(jobId);
        detail.setSourceCsvPath(sourceFile.toString());
        detail.setStatus(DetailStatus.TRANSCODING);

        Qianyi qianyi = new Qianyi();
        qianyi.setId(qianyiId);
        qianyi.setJobId(jobId);
        qianyi.setDdlFilePath("schema.sql");

        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));
        when(qianyiRepo.findById(qianyiId)).thenReturn(Optional.of(qianyi));

        // 5. Mock 工具类
        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(any())).thenReturn(Collections.emptyList());
        
        // 【关键】Mock CharsetFactory
        // 我们用 UTF-8 模拟 IBM-1388，这样我们写入的测试文件可以直接被读取，
        // 从而避开了构造真实 IBM-1388 字节流的复杂性。
        charsetFactoryMock.when(() -> CharsetFactory.resolveCharset(any())).thenReturn(StandardCharsets.UTF_8);
        charsetFactoryMock.when(() -> CharsetFactory.createDecoder(any(), anyBoolean())).thenCallRealMethod();

        // 【关键】配置 FastEscapeHandler 调用真实方法！
        // 这样我们才能测试 unescape 的真实逻辑
        //fastEscapeHandlerMock.when(() -> FastEscapeHandler.unescape(anyString())).thenCallRealMethod();

        // 6. Mock AppProperties (控制 Tunneling 开关)
        mockAppConfig("IBM-1388", enableTunneling);

        // 7. 执行
        transcodeService.execute(detailId);

        // 8. 返回结果内容供断言
        if (Files.exists(splitFile)) {
            return Files.readString(splitFile, StandardCharsets.UTF_8);
        }
        return "";
    }

    private void mockAppConfig(String encoding, boolean tunneling) {
        AppProperties.CsvDetailConfig ibmConfig = new AppProperties.CsvDetailConfig();
        ibmConfig.setEncoding(encoding);
        ibmConfig.setTunneling(tunneling); // 【控制点】

        AppProperties.CsvDetailConfig utf8Config = new AppProperties.CsvDetailConfig();
        utf8Config.setEncoding("UTF-8");

        AppProperties.Csv csv = new AppProperties.Csv();
        csv.setIbmSource(ibmConfig);
        csv.setUtf8Split(utf8Config);

        AppProperties.Performance perf = new AppProperties.Performance();
        perf.setReadBufferSize(1024);
        perf.setWriteBufferSize(1024);
        perf.setSplitRows(1000);

        AppProperties.TranscodeJob jobConfig = new AppProperties.TranscodeJob();
        jobConfig.setMaxErrorCount(10);

        lenient().when(config.getCsv()).thenReturn(csv);
        lenient().when(config.getPerformance()).thenReturn(perf);
        lenient().when(config.getTranscodeJob()).thenReturn(jobConfig);
    }
}