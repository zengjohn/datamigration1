package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.VerifyStrategy;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.example.moveprog.repository.QianyiRepository;
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

import java.io.*;
import java.nio.charset.StandardCharsets;
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
    private static final String CHARSET_IBM = "x-IBM1388";

    @Mock private StateManager stateManager;
    @Mock private MigrationJobRepository jobRepo;
    @Mock private QianyiRepository qianyiRepository;
    @Mock private QianyiDetailRepository detailRepo;
    @Mock private CsvSplitRepository splitRepo;
    @Mock private CoreComparator coreComparator;
    @Mock private TargetDatabaseConnectionManager targetDatabaseConnectionManager;
    @Mock MigrationArtifactManager migrationArtifactManager;
    @Mock JdbcHelper jdbcHelper;
    @Mock private AppProperties config;

    @InjectMocks
    private VerifyService verifyService;

    @TempDir Path tempDir;

    private MockedStatic<SchemaParseUtil> schemaParseUtilMock;

    @BeforeEach
    void setUp() {
        schemaParseUtilMock = Mockito.mockStatic(SchemaParseUtil.class);
    }

    @AfterEach
    void tearDown() {
        schemaParseUtilMock.close();
    }

    /**
     * 测试：Split 2 (StartRow=4) 是否能正确读取到第4行
     * 场景：文件共5行，Split 2 从第4行开始，由 VerifyService 读取源文件进行校验
     */
    @Test
    void testExecute_SecondSplit_RowAlignment() throws Exception {
        Long splitId = 200L;
        Long detailId = 10L;
        Long qianyiId = 100L;
        Long jobId = 1L;

        // 2. 模拟 Split 2 (StartRow=4, Count=2)
        CsvSplit split = new CsvSplit();
        split.setId(splitId);
        split.setJobId(jobId);
        split.setQianyiId(qianyiId);
        split.setDetailId(detailId);
        split.setStatus(CsvSplitStatus.VERIFYING);
        split.setStartRowNo(5L); // 【关键】从第5行开始
        split.setRowCount(3L);   // 读3行 (val5, val6, val7)

        // 3. Mock 依赖
        when(splitRepo.findById(splitId)).thenReturn(Optional.of(split));

        // Mock Job (VerifyService usually needs job info for paths)
        MigrationJob job = new MigrationJob();
        job.setId(jobId);
        job.setOutDirectory(tempDir.toString());
        // Use lenient() because some verify strategies might not access jobRepo, preventing UnnecessaryStubbingException
        lenient().when(jobRepo.findById(jobId)).thenReturn(Optional.of(job));

        // 1. Prepare source file (7 lines)
        Path sourceFile = tempDir.resolve("ibm1388_source.csv");
        String content = "val1\nval2\nval3\nval4\nval5\nval6\nval7";
        writeWithEncoding(sourceFile, content, CHARSET_IBM);

        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setQianyiId(qianyiId);
        detail.setJobId(jobId);
        detail.setSourceCsvPath(sourceFile.toString());
        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));

        // Mock DDL 解析
        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(any())).thenReturn(Collections.emptyList());

        // Mock DB 迭代器 (为了让代码跑到 createFileIterator，必须让 DB 部分不报错)
        Connection mockConn = mock(Connection.class);
        Statement mockStmt = mock(Statement.class);
        ResultSet mockRs = mock(ResultSet.class);
        ResultSetMetaData mockMeta = mock(ResultSetMetaData.class);

        // 1. Mock Connection -> Statement
        lenient().when(targetDatabaseConnectionManager.getConnection(anyLong(), anyBoolean())).thenReturn(mockConn);
        // Important: Match any generic createStatement call
        lenient().when(mockConn.createStatement(anyInt(), anyInt())).thenReturn(mockStmt);
        lenient().when(mockConn.createStatement()).thenReturn(mockStmt);

        // 2. Mock Statement -> ResultSet
        // This was the cause of NPE. We ensure ANY executeQuery returns our mockRs
        lenient().when(mockStmt.executeQuery(any())).thenReturn(mockRs);

        // 3. Mock ResultSet Metadata (prevent NPE in JdbcRowIterator constructor)
        lenient().when(mockRs.getMetaData()).thenReturn(mockMeta);
        lenient().when(mockMeta.getColumnCount()).thenReturn(1);
        lenient().when(mockMeta.getColumnType(anyInt())).thenReturn(java.sql.Types.VARCHAR);
        lenient().when(mockMeta.getColumnLabel(anyInt())).thenReturn("col_dummy");

        // Mock 配置 (使用源文件策略)
        mockAppConfig_VerifySourceFile();

        // 【核心】拦截 coreComparator，查看 VerifyService 传给它的 fileIterator 到底读到了什么
        when(coreComparator.compareStreams(anyLong(), any(), any(), any())).thenAnswer(inv -> {
            CloseableRowIterator<String> fileIter = inv.getArgument(1);

            // Verify Split 2 reads correctly (should start from "val4")
            String[] row1 = fileIter.hasNext() ? fileIter.next() : null;
            String[] row2 = fileIter.hasNext() ? fileIter.next() : null;

            assertNotNull(row1, "Should read fifth row of source");
            assertEquals("val5", row1[0], "fourth row should be line 5 (val5)");
            assertEquals("5", row1[1], "fourth row should be line 5 (val5)");


            assertNotNull(row2, "Should read sixth row of source");
            assertEquals("val6", row2[0], "sixth row should be line 6 (val6)");
            assertEquals("6", row2[1], "sixth row should be line 6 (val6)");

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
        ibmConfig.setEncoding("IBM1388");
        ibmConfig.setLineSeparator("\n");
        ibmConfig.setDelimiter(',');
        ibmConfig.setTunneling(false);
        ibmConfig.setHeaderExtraction(false);
        ibmConfig.setSkipRows(0L);

        AppProperties.Csv csv = new AppProperties.Csv();
        csv.setIbmSource(ibmConfig);

        lenient().when(config.getVerify()).thenReturn(verifyConfig);
        lenient().when(config.getCsv()).thenReturn(csv);
    }

    private static void writeWithEncoding(Path path, String content, String encoding) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(path.toFile()), encoding))) {
            writer.write(content);
        }
    }

}