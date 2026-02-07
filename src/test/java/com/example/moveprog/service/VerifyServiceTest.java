package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.VerifyStrategy;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.example.moveprog.repository.QianyiRepository;
import com.example.moveprog.util.CharsetFactory;
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class VerifyServiceTest {

    @Mock private StateManager stateManager;
    @Mock private MigrationJobRepository jobRepo;
    @Mock private QianyiRepository qianyiRepo;
    @Mock private QianyiDetailRepository detailRepo;
    @Mock private CsvSplitRepository splitRepo;
    @Mock private CoreComparator coreComparator;
    @Mock private TargetDatabaseConnectionManager targetDatabaseConnectionManager;
    @Mock private AppProperties config;

    @InjectMocks
    private VerifyService verifyService;

    @TempDir Path tempDir;

    private MockedStatic<CharsetFactory> charsetFactoryMock;
    private MockedStatic<SchemaParseUtil> schemaParseUtilMock;

    @BeforeEach
    void setUp() {
        charsetFactoryMock = Mockito.mockStatic(CharsetFactory.class);
        schemaParseUtilMock = Mockito.mockStatic(SchemaParseUtil.class);
    }

    @AfterEach
    void tearDown() {
        charsetFactoryMock.close();
        schemaParseUtilMock.close();
    }

    /**
     * 测试：Split 2 (StartRow=4) 是否能正确读取到第4行
     * 场景：文件共5行，Split 2 从第4行开始，由 VerifyService 读取源文件进行校验
     */
    @Test
    void testExecute_SecondSplit_RowAlignment() throws Exception {
        // 1. 准备源文件 (5行纯数据)
        // L1: val1
        // L2: val2
        // L3: val3
        // L4: val4  <-- 假设我们想测 Split 2，从第4行开始
        // L5: val5
        Path sourceFile = tempDir.resolve("source.csv");
        String content = "val1\nval2\nval3\nval4\nval5";
        Files.writeString(sourceFile, content);

        // 2. 模拟 Split 2 (StartRow=4, Count=2)
        Long splitId = 200L;
        CsvSplit split = new CsvSplit();
        split.setId(splitId);
        split.setJobId(1L);
        split.setDetailId(10L);
        split.setQianyiId(100L);
        split.setStatus(CsvSplitStatus.VERIFYING);
        split.setStartRowNo(4L); // 【关键】从第4行开始
        split.setRowCount(2L);   // 读2行 (val3, val4)

        // 3. Mock 依赖
        when(splitRepo.findById(splitId)).thenReturn(Optional.of(split));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(new MigrationJob()));

        // 【修正点】创建一个包含 outDirectory 的 Job 对象
        MigrationJob job = new MigrationJob();
        job.setId(1L);
        job.setOutDirectory(tempDir.toString()); // 设置临时目录，防止 NPE
        when(jobRepo.findById(1L)).thenReturn(Optional.of(job));

        QianyiDetail detail = new QianyiDetail();
        detail.setSourceCsvPath(sourceFile.toString());
        when(detailRepo.findById(10L)).thenReturn(Optional.of(detail));

        Qianyi qianyi = new Qianyi();
        qianyi.setDdlFilePath("ddl.sql");
        qianyi.setTargetTableName("t_target");
        when(qianyiRepo.findById(100L)).thenReturn(Optional.of(qianyi));

        // Mock DDL 解析
        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(any())).thenReturn(Collections.emptyList());

        // Mock DB 迭代器 (为了让代码跑到 createFileIterator，必须让 DB 部分不报错)
        Connection mockConn = mock(Connection.class);
        Statement mockStmt = mock(Statement.class);
        ResultSet mockRs = mock(ResultSet.class);

        ResultSetMetaData mockMeta = mock(ResultSetMetaData.class);
        when(mockRs.getMetaData()).thenReturn(mockMeta);
        when(mockMeta.getColumnCount()).thenReturn(1);
        when(mockMeta.getColumnType(anyInt())).thenReturn(java.sql.Types.VARCHAR);
        when(mockMeta.getColumnLabel(anyInt())).thenReturn("col_dummy");

        // 【关键修改】适配您修改后的 getConnection(Long, boolean) 方法
        // 无论是 true 还是 false，都返回 mockConn
        lenient().when(targetDatabaseConnectionManager.getConnection(anyLong(), anyBoolean())).thenReturn(mockConn);
        
        // 假设 JdbcRowIterator 构造时会创建 Statement
        lenient().when(mockConn.createStatement(anyInt(), anyInt())).thenReturn(mockStmt);
        lenient().when(mockStmt.executeQuery(anyString())).thenReturn(mockRs);

        // Mock 配置 (使用源文件策略)
        mockAppConfig_VerifySourceFile();

        // Mock Charset
        charsetFactoryMock.when(() -> CharsetFactory.resolveCharset(any())).thenReturn(StandardCharsets.UTF_8);

        // 【核心】拦截 coreComparator，查看 VerifyService 传给它的 fileIterator 到底读到了什么
        when(coreComparator.compareStreams(any(), any(), any(), any())).thenAnswer(inv -> {
            CloseableRowIterator<String> fileIter = inv.getArgument(1);
            
            // 读取迭代器的第一行
            String[] row1 = null;
            if (fileIter.hasNext()) row1 = fileIter.next();
            
            // 读取迭代器的第二行
            String[] row2 = null;
            if (fileIter.hasNext()) row2 = fileIter.next();

            // --- 断言验证 ---
            assertNotNull(row1, "Split 2 应该读到数据");
            
            assertEquals("val4", row1[0], "Split 2 的第一行应该是 'val4' (Line 4)");

            assertNotNull(row2, "Split 2 应该读到第二行");
            assertEquals("val5", row2[0], "Split 2 的第二行应该是 'val5' (Line 5)");

            return 0L; // 模拟无差异
        });

        // 执行测试
        verifyService.execute(splitId);
    }

    private void mockAppConfig_VerifySourceFile() {
        AppProperties.Verify verifyConfig = new AppProperties.Verify();
        verifyConfig.setStrategy(VerifyStrategy.USE_SOURCE_FILE); // 【关键】使用读源文件策略
        verifyConfig.setFetchSize(100);

        AppProperties.CsvDetailConfig ibmConfig = new AppProperties.CsvDetailConfig();
        ibmConfig.setEncoding("UTF-8");
        ibmConfig.setTunneling(false);

        AppProperties.Csv csv = new AppProperties.Csv();
        csv.setIbmSource(ibmConfig);

        lenient().when(config.getVerify()).thenReturn(verifyConfig);
        lenient().when(config.getCsv()).thenReturn(csv);
    }
}