package com.example.moveprog.integration;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.config.StartupValidator;
import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.BatchStatus;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.enums.JobStatus;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.example.moveprog.repository.QianyiRepository;
import com.example.moveprog.scheduler.DirectoryMonitor;
import com.example.moveprog.scheduler.MigrationDispatcher;
import com.example.moveprog.service.LoadService;
import com.example.moveprog.service.TargetDatabaseConnectionManager;
import com.example.moveprog.service.TranscodeService;
import com.example.moveprog.service.VerifyService;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
@Testcontainers(disabledWithoutDocker = true)
public class IntegrationTesting {
    private static final String CHARSET_IBM = "x-IBM1388";

    @Container
    protected static final MySQLContainer<?> MYSQL_CONTAINER = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("test1")
            .withUsername("root")
            .withPassword("111111")
            .withReuse(true);

    @Container
    protected static final MySQLContainer<?> TARGET_CONTAINER = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("test2")
            .withUsername("test")
            .withPassword("testpasswd")
            .withReuse(true);

    @DynamicPropertySource
    static void registerMySQLProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL_CONTAINER::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL_CONTAINER::getUsername);
        registry.add("spring.datasource.password", MYSQL_CONTAINER::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("spring.jpa.defer-datasource-initialization", () -> "true");
    }

    @MockBean private DirectoryMonitor directoryMonitor;
    @MockBean private MigrationDispatcher migrationDispatcher;
    @MockBean private StartupValidator startupValidator;

    @SpyBean
    private AppProperties appProperties;

    @Autowired private MigrationJobRepository migrationJobRepository;
    @Autowired private QianyiRepository qianyiRepository;
    @Autowired private QianyiDetailRepository  qianyiDetailRepository;
    @Autowired private CsvSplitRepository csvSplitRepository;

    @Autowired private TargetDatabaseConnectionManager targetDatabaseConnectionManager;
    @Autowired private TranscodeService transcodeService;
    @Autowired private LoadService loadService;
    @Autowired private VerifyService verifyService;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("整个流程成功跑完")
    void shouldVerifySuccessfully() throws IOException, SQLException {
        shouldVerifySuccessfully(false);
    }

    @Test
    @DisplayName("有空行, 整个流程成功跑完")
    void shouldVerifySuccessfully_withEmptyLine() throws IOException, SQLException {
        shouldVerifySuccessfully(true);
    }

    void shouldVerifySuccessfully(boolean hasEmptyLine) throws IOException, SQLException {

        String tblName = "t1";

        Pair<MigrationJob,Long> pair = generateData(tblName, (sourceCsvPath) -> {
            List<List<String>> rows = new ArrayList<>();
            Long totalDataRows = 11L;
            StringBuilder csvContent = new StringBuilder();
            if (hasEmptyLine) {
                for(int i=0; i<5; i++) {
                    csvContent.append("\n"); // 添加空行
                }
            }
            for(int i=0; i < totalDataRows; i++) {
                if (hasEmptyLine) {
                    if (2==i || 5==i) {
                        csvContent.append("\n"); // 添加空行
                    }
                }
                List<String> row = generateRowData(i + 125L);
                rows.add(row);
                csvContent.append(row.stream().collect(Collectors.joining(",")));
                if (i < totalDataRows - 1) {
                    csvContent.append("\n");
                }
            }
            if (hasEmptyLine) {
                csvContent.append("\n"); // 添加空行
            }
            try {
                writeWithEncoding(sourceCsvPath.toString(), csvContent.toString(), CHARSET_IBM);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Long detailId = pair.getValue();
        MigrationJob job = pair.getKey();

        AppProperties.Performance perf = new AppProperties.Performance();
        perf.setSplitRows(3);
        doReturn(perf).when(appProperties).getPerformance();

        // 目标端建表
        String tableName = String.format("`%s`.`%s`", "test2", tblName);
        try(Connection connection = targetDatabaseConnectionManager.getConnection(job.getId(), false)) {
            createTableSql(connection, tableName);
        }

        transcodeService.execute(detailId);

        QianyiDetail qianyiDetailById = qianyiDetailRepository.findById(detailId).orElseThrow();
        assertEquals(DetailStatus.PROCESSING_CHILDS, qianyiDetailById.getStatus());
        Long transcodeErrorCount = qianyiDetailById.getTranscodeErrorCount();
        assertEquals(0, transcodeErrorCount);

        List<CsvSplit> csvSplitsByDetailId = csvSplitRepository.findByDetailId(qianyiDetailById.getId());
        assertEquals(4, csvSplitsByDetailId.size());


        for (CsvSplit csvSplit : csvSplitsByDetailId) {
            // 更新为正在装载
            Long splitId = csvSplit.getId();
            csvSplitRepository.updateStatus(splitId, CsvSplitStatus.WAIT_LOAD, CsvSplitStatus.LOADING);
            loadService.execute(splitId);
            CsvSplit csvSplit1 = csvSplitRepository.findById(splitId).orElseThrow();
            assertEquals(CsvSplitStatus.WAIT_VERIFY, csvSplit1.getStatus());

            // 更新为正在验证
            csvSplitRepository.updateStatus(splitId, CsvSplitStatus.WAIT_VERIFY, CsvSplitStatus.VERIFYING);
            verifyService.execute(splitId);
            csvSplit1 = csvSplitRepository.findById(splitId).orElseThrow();
            assertEquals(CsvSplitStatus.PASS, csvSplit1.getStatus());
        }


        qianyiDetailById = qianyiDetailRepository.findById(detailId).orElseThrow();
        assertEquals(DetailStatus.FINISHED, qianyiDetailById.getStatus());

    }

    @Test
    @DisplayName("有乱码行, 修整csv文件后可以patch成功")
    void shouldPatchSuccessfully() throws IOException, SQLException {
        String tblName = "t11";

        Pair<MigrationJob,Long> pair = generateData(tblName, (sourceCsvPath) -> {

            try (FileOutputStream fos = new FileOutputStream(sourceCsvPath.toString())) {
                for(int i=0; i < 3; i++) {
                    List<String> row = generateRowData(i + 125L);
                    String str = row.stream().collect(Collectors.joining(",")) + "\n";
                    fos.write(str.getBytes(CHARSET_IBM));
                }

                // 有乱码
                {
                    List<String> row = generateRowData(3 + 125L);
                    for (int i = 0; i < row.size(); i++) {
                        if (1 != i) {
                            fos.write(row.get(i).getBytes(CHARSET_IBM));
                        } else {
                            // 乱码
                            fos.write(new byte[]{(byte) 0x0E, 0x45, 0x00, (byte) 0x0F});
                        }

                        if (i == row.size() - 1) {
                            fos.write("\n".getBytes(CHARSET_IBM));
                        } else {
                            fos.write(",".getBytes(CHARSET_IBM));
                        }
                    }
                }

                {
                    List<String> row = generateRowData(4 + 125L);
                    String str = row.stream().collect(Collectors.joining(",")) + "\n";
                    fos.write(str.getBytes(CHARSET_IBM));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        AppProperties.Performance perf = new AppProperties.Performance();
        perf.setSplitRows(3);
        doReturn(perf).when(appProperties).getPerformance();

        Long detailId = pair.getValue();
        MigrationJob job = pair.getKey();

        // 目标端建表
        String tableName = String.format("`%s`.`%s`", "test2", tblName);
        try(Connection connection = targetDatabaseConnectionManager.getConnection(job.getId(), false)) {
            createTableSql(connection, tableName);
        }

        transcodeService.execute(detailId);

        QianyiDetail qianyiDetailById = qianyiDetailRepository.findById(detailId).orElseThrow();
        assertEquals(DetailStatus.PROCESSING_CHILDS, qianyiDetailById.getStatus());
        Long transcodeErrorCount = qianyiDetailById.getTranscodeErrorCount();
        assertEquals(1, transcodeErrorCount);

        List<CsvSplit> csvSplitsByDetailId = csvSplitRepository.findByDetailId(qianyiDetailById.getId());
        assertEquals(2, csvSplitsByDetailId.size());

        for (CsvSplit csvSplit : csvSplitsByDetailId) {
            // 更新为正在装载
            Long splitId = csvSplit.getId();
            csvSplitRepository.updateStatus(splitId, CsvSplitStatus.WAIT_LOAD, CsvSplitStatus.LOADING);
            loadService.execute(splitId);
            CsvSplit csvSplit1 = csvSplitRepository.findById(splitId).orElseThrow();
            assertEquals(CsvSplitStatus.WAIT_VERIFY, csvSplit1.getStatus());

            // 更新为正在验证
            csvSplitRepository.updateStatus(splitId, CsvSplitStatus.WAIT_VERIFY, CsvSplitStatus.VERIFYING);
            verifyService.execute(splitId);
            csvSplit1 = csvSplitRepository.findById(splitId).orElseThrow();
            assertEquals(CsvSplitStatus.PASS, csvSplit1.getStatus());
        }

        qianyiDetailById = qianyiDetailRepository.findById(detailId).orElseThrow();
        assertEquals(DetailStatus.FINISHED, qianyiDetailById.getStatus());

    }

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

    private void createTableSql(Connection connection, String tableName) throws SQLException {
        try(Statement statement = connection.createStatement()) {

            statement.execute("DROP TABLE IF EXISTS " + tableName);

            StringBuilder ddl = new StringBuilder();
            ddl.append("CREATE TABLE ").append(tableName).append(" (");
            ddl.append("id int,");
            ddl.append("name varchar(32),");
            ddl.append("remark varchar(64),");
            ddl.append("weight double,");
            ddl.append("salary decimal(10,2),");
            ddl.append("birth date,");
            ddl.append("birthday datetime(6),");
            ddl.append("luncherat time(6),");
            ddl.append("csv_id bigint,");
            ddl.append("source_row_no bigint,");
            ddl.append("KEY idx_csvid_rowno (csv_id,source_row_no)");
            ddl.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

            statement.execute(ddl.toString());
        }
    }

    private String generateDdl() {
        StringBuilder ddl = new StringBuilder();
        ddl.append("id,int\n");
        ddl.append("name,varchar\n");
        ddl.append("remark,varchar\n");
        ddl.append("weight,double\n");
        ddl.append("salary,decimal\n");
        ddl.append("birth,date\n");
        ddl.append("birthday,datetime\n");
        ddl.append("luncherat,time\n");
        return ddl.toString();
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

    private static void writeWithEncoding(String path, String content, String encoding) throws IOException {
        File file = new File(path);
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file), encoding))) {
            writer.write(content);
            System.out.println("生成成功: " + path);
            System.out.println("文件大小: " + file.length() + " 字节");
        }
    }


    private Pair<MigrationJob,Long> generateData(String tblName, Consumer<Path> consumerCsv) throws IOException {
        Path sourceDir = tempDir.resolve("sourceDir");
        Path outDir = tempDir.resolve("outDir");

        // 迁移作业
        MigrationJob job = new MigrationJob();
        job.setName("test1");
        job.setSourceDirectory(sourceDir.toString());
        job.setOutDirectory(outDir.toString());
        job.setTargetUrl(TARGET_CONTAINER.getJdbcUrl());
        job.setTargetDbHost("dummyHost");
        job.setTargetDbPort(3316);
        job.setTargetDbUser(TARGET_CONTAINER.getUsername());
        job.setTargetDbPass(TARGET_CONTAINER.getPassword());
        job.setTargetSchema(TARGET_CONTAINER.getDatabaseName());
        job.setStatus(JobStatus.ACTIVE);
        migrationJobRepository.save(job);

        // 迁移申请(对于ok文件)
        Path okFilePath = tempDir.resolve("t2.ok");
        Path ddlFilePath = tempDir.resolve(tblName);
        Files.writeString(ddlFilePath, generateDdl());
        Qianyi qianyi =  new Qianyi();
        qianyi.setJobId(job.getId());
        qianyi.setOkFilePath(okFilePath.toString());
        qianyi.setDdlFilePath(ddlFilePath.toString());
        qianyi.setTargetSchema("test2");
        qianyi.setTargetTableName(tblName);
        qianyi.setStatus(BatchStatus.PROCESSING);
        qianyiRepository.save(qianyi);

        // 迁移明细
        Path sourceCsvPath =  tempDir.resolve("ibm1388_source2.csv");
        consumerCsv.accept(sourceCsvPath);

        QianyiDetail detail1 = new QianyiDetail();
        detail1.setJobId(job.getId());
        detail1.setQianyiId(qianyi.getId());
        detail1.setSourceCsvPath(sourceCsvPath.toString());
        detail1.setTargetSchema(qianyi.getTargetSchema());
        detail1.setTargetTableName(qianyi.getTargetTableName());
        detail1.setStatus(DetailStatus.TRANSCODING);
        qianyiDetailRepository.save(detail1);

        return Pair.of(job,detail1.getId());
    }
}
