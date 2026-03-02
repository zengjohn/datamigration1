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
import com.example.moveprog.service.impl.CsvRowIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.sql.*;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
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
    @DisplayName("源ibm1388 csv文件和数据库比较")
    void testExecute_source() throws Exception {
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
        String content = "val1,t1\nval2,t2\nval3,t3\nval4,t4\nval5,t5\nval6,t6\nval7,t7";
        writeWithEncoding(sourceFile, content, CHARSET_IBM);

        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setQianyiId(qianyiId);
        detail.setJobId(jobId);
        detail.setSourceCsvPath(sourceFile.toString());
        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));

        // Mock DDL 解析
        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(any())).thenReturn(Collections.emptyList());

        // 【关键修复 1】Mock JdbcHelper，防止 sqlPair.getKey() 空指针
        // 注意：这里需要 import org.apache.commons.lang3.tuple.Pair;
        when(jdbcHelper.verifySelectSql(anyLong())).thenReturn(org.apache.commons.lang3.tuple.Pair.of("SELECT 'mock_sql'", Collections.emptyList()));

        // Mock DB 迭代器 (为了让代码跑到 createFileIterator，必须让 DB 部分不报错)
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPrepareStmt = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        ResultSetMetaData mockMeta = mock(ResultSetMetaData.class);

        // 1. Mock Connection -> Statement
        lenient().when(targetDatabaseConnectionManager.getConnection(anyLong(), anyBoolean())).thenReturn(mockConn);
        // Important: Match any generic createStatement call
        lenient().when(mockConn.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(mockPrepareStmt);
        lenient().when(mockConn.prepareStatement(anyString())).thenReturn(mockPrepareStmt);

        // 2. Mock Statement -> ResultSet
        // This was the cause of NPE. We ensure ANY executeQuery returns our mockRs
        // 【关键修复 2】Mock executeQuery
        // 如果你代码改为了 ps.executeQuery() (无参)，之前的 mock(any()) 就不生效了
        // 所以这里同时 Mock 无参和有参两种情况，确保万无一失
        lenient().when(mockPrepareStmt.executeQuery()).thenReturn(mockRs);           // 对应 ps.executeQuery()
        lenient().when(mockPrepareStmt.executeQuery(anyString())).thenReturn(mockRs); // 对应 ps.executeQuery(sql)

        // 3. Mock ResultSet Metadata (prevent NPE in JdbcRowIterator constructor)
        lenient().when(mockRs.getMetaData()).thenReturn(mockMeta);
        lenient().when(mockMeta.getColumnCount()).thenReturn(2); // 两列
        lenient().when(mockMeta.getColumnType(anyInt())).thenReturn(java.sql.Types.VARCHAR);
        lenient().when(mockMeta.getColumnLabel(anyInt())).thenReturn("col_dummy");

        // Mock 配置 (使用源文件策略)
        mockAppConfig_VerifySourceFile(VerifyStrategy.USE_SOURCE_FILE);

        // 【核心】拦截 coreComparator，查看 VerifyService 传给它的 fileIterator 到底读到了什么
        when(coreComparator.compareStreams(anyLong(), any(), any(), any())).thenAnswer(inv -> {
            CloseableRowIterator<String> fileIter = inv.getArgument(1);

            Field fieldRrowNumberOffset = CsvRowIterator.class.getDeclaredField("rowNumberOffset");
            fieldRrowNumberOffset.setAccessible(true);
            long rowNumberOffset = (long)fieldRrowNumberOffset.get(fileIter);
            assertEquals(0, rowNumberOffset);

            Field fieldFilePath = CsvRowIterator.class.getDeclaredField("filePath");
            fieldFilePath.setAccessible(true);
            String filePath = (String)fieldFilePath.get(fileIter);
            assertEquals(sourceFile.toString(), filePath);

            Field fieldSplitCsvFile = CsvRowIterator.class.getDeclaredField("splitCsvFile");
            fieldSplitCsvFile.setAccessible(true);
            boolean splitCsvFile = (boolean)fieldSplitCsvFile.get(fileIter);
            assertFalse(splitCsvFile);

            // Verify Split 2 reads correctly (should start from "val4")
            String[] row1 = fileIter.hasNext() ? fileIter.next() : null;
            String[] row2 = fileIter.hasNext() ? fileIter.next() : null;

            assertNotNull(row1, "Should read fifth row of source");
            assertEquals("val5", row1[0], "fourth row should be line 5 (val5)");
            assertEquals("t5", row1[1], "fourth row should be line 5 (val5)");
            assertEquals("5", row1[2], "fourth row should be line 5 (val5)");


            assertNotNull(row2, "Should read sixth row of source");
            assertEquals("val6", row2[0], "sixth row should be line 6 (val6)");
            assertEquals("t6", row2[1], "sixth row should be line 6 (val6)");
            assertEquals("6", row2[2], "sixth row should be line 6 (val6)");

            return 0L; // 模拟无差异
        });

        // 执行测试
        verifyService.execute(splitId);
    }

    @Test
    @DisplayName("拆分csv utf8文件和数据库比较")
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
        Path splitCsv = tempDir.resolve("split_utf8.csv");
        String content = "val5,t5,5\nval6,t6,6\nval7,t7,7";
        writeWithEncoding(splitCsv, content, "UTF-8");
        split.setSplitFilePath(splitCsv.toString());

        // 3. Mock 依赖
        when(splitRepo.findById(splitId)).thenReturn(Optional.of(split));

        // Mock Job (VerifyService usually needs job info for paths)
        MigrationJob job = new MigrationJob();
        job.setId(jobId);
        job.setOutDirectory(tempDir.resolve("out_home").toString());
        // Use lenient() because some verify strategies might not access jobRepo, preventing UnnecessaryStubbingException
        lenient().when(jobRepo.findById(jobId)).thenReturn(Optional.of(job));

        Path source = tempDir.resolve("ibm1388.csv");
        // 1. Prepare source file (7 lines)
        QianyiDetail detail = new QianyiDetail();
        detail.setId(detailId);
        detail.setQianyiId(qianyiId);
        detail.setJobId(jobId);
        detail.setSourceCsvPath(source.toString());
        when(detailRepo.findById(detailId)).thenReturn(Optional.of(detail));

        // Mock DDL 解析
        schemaParseUtilMock.when(() -> SchemaParseUtil.parseColumnNamesFromDdl(any())).thenReturn(Collections.emptyList());

        // 【关键修复 1】Mock JdbcHelper，防止 sqlPair.getKey() 空指针
        // 注意：这里需要 import org.apache.commons.lang3.tuple.Pair;
        when(jdbcHelper.verifySelectSql(anyLong())).thenReturn(org.apache.commons.lang3.tuple.Pair.of("SELECT 'mock_sql'", Collections.emptyList()));

        // Mock DB 迭代器 (为了让代码跑到 createFileIterator，必须让 DB 部分不报错)
        Connection mockConn = mock(Connection.class);
        PreparedStatement mockPrepareStmt = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        ResultSetMetaData mockMeta = mock(ResultSetMetaData.class);

        // 1. Mock Connection -> Statement
        lenient().when(targetDatabaseConnectionManager.getConnection(anyLong(), anyBoolean())).thenReturn(mockConn);
        // Important: Match any generic createStatement call
        lenient().when(mockConn.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(mockPrepareStmt);
        lenient().when(mockConn.prepareStatement(anyString())).thenReturn(mockPrepareStmt);

        // 2. Mock Statement -> ResultSet
        // This was the cause of NPE. We ensure ANY executeQuery returns our mockRs
        // 【关键修复 2】Mock executeQuery
        // 如果你代码改为了 ps.executeQuery() (无参)，之前的 mock(any()) 就不生效了
        // 所以这里同时 Mock 无参和有参两种情况，确保万无一失
        lenient().when(mockPrepareStmt.executeQuery()).thenReturn(mockRs);           // 对应 ps.executeQuery()
        lenient().when(mockPrepareStmt.executeQuery(anyString())).thenReturn(mockRs); // 对应 ps.executeQuery(sql)

        // 3. Mock ResultSet Metadata (prevent NPE in JdbcRowIterator constructor)
        lenient().when(mockRs.getMetaData()).thenReturn(mockMeta);
        lenient().when(mockMeta.getColumnCount()).thenReturn(2); // 两列
        lenient().when(mockMeta.getColumnType(anyInt())).thenReturn(java.sql.Types.VARCHAR);
        lenient().when(mockMeta.getColumnLabel(anyInt())).thenReturn("col_dummy");

        // Mock 配置 (使用源文件策略)
        mockAppConfig_VerifySourceFile(VerifyStrategy.USE_UTF8_SPLIT);

        // 【核心】拦截 coreComparator，查看 VerifyService 传给它的 fileIterator 到底读到了什么
        when(coreComparator.compareStreams(anyLong(), any(), any(), any())).thenAnswer(inv -> {
            CsvRowIterator fileIter = inv.getArgument(1);

            Field fieldRrowNumberOffset = CsvRowIterator.class.getDeclaredField("rowNumberOffset");
            fieldRrowNumberOffset.setAccessible(true);
            long rowNumberOffset = (long)fieldRrowNumberOffset.get(fileIter);
            assertEquals(5, rowNumberOffset);

            Field fieldFilePath = CsvRowIterator.class.getDeclaredField("filePath");
            fieldFilePath.setAccessible(true);
            String filePath = (String)fieldFilePath.get(fileIter);
            assertEquals(splitCsv.toString(), filePath);

            Field fieldSplitCsvFile = CsvRowIterator.class.getDeclaredField("splitCsvFile");
            fieldSplitCsvFile.setAccessible(true);
            boolean splitCsvFile = (boolean)fieldSplitCsvFile.get(fileIter);
            assertTrue(splitCsvFile);

            // Verify Split 2 reads correctly (should start from "val4")
            String[] row1 = fileIter.hasNext() ? fileIter.next() : null;
            String[] row2 = fileIter.hasNext() ? fileIter.next() : null;

            assertNotNull(row1, "Should read fifth row of source");
            assertEquals("val5", row1[0], "fourth row should be line 5 (val5)");
            assertEquals("t5", row1[1], "fourth row should be line 5 (val5)");
            assertEquals("5", row1[2], "fourth row should be line 5 (val5)");


            assertNotNull(row2, "Should read sixth row of source");
            assertEquals("val6", row2[0], "sixth row should be line 6 (val6)");
            assertEquals("t6", row2[1], "sixth row should be line 6 (val6)");
            assertEquals("6", row2[2], "sixth row should be line 6 (val6)");

            return 0L; // 模拟无差异
        });

        // 执行测试
        verifyService.execute(splitId);
    }

    private void mockAppConfig_VerifySourceFile(VerifyStrategy verifyStrategy) {
        AppProperties.Verify verifyConfig = new AppProperties.Verify();
        verifyConfig.setStrategy(verifyStrategy);
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