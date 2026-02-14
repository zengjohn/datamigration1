package com.example.moveprog.integration;

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
import com.example.moveprog.service.TranscodeService;
import com.example.moveprog.service.VerifyService;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    @Autowired private MigrationJobRepository migrationJobRepository;
    @Autowired private QianyiRepository qianyiRepository;
    @Autowired private QianyiDetailRepository  qianyiDetailRepository;
    @Autowired private CsvSplitRepository csvSplitRepository;

    @Autowired private TranscodeService transcodeService;
    @Autowired private LoadService loadService;
    @Autowired private VerifyService verifyService;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("整个流程应该成功跑完")
    void shouldVerifySuccessfully() throws IOException {
        Path sourceDir = tempDir.resolve("sourceDir");
        Path outDir = tempDir.resolve("outDir");

        // 迁移作业
        MigrationJob job = new MigrationJob();
        job.setName("test1");
        job.setSourceDirectory(sourceDir.toString());
        job.setOutDirectory(outDir.toString());
        job.setTargetDbHost(MYSQL_CONTAINER.getJdbcUrl());
        job.setTargetDbUser(MYSQL_CONTAINER.getUsername());
        job.setTargetDbPass(MYSQL_CONTAINER.getPassword());
        job.setTargetSchema(MYSQL_CONTAINER.getDatabaseName());
        job.setStatus(JobStatus.ACTIVE);
        migrationJobRepository.save(job);

        // 迁移申请(对于ok文件)
        Path okFilePath = tempDir.resolve("t1.ok");
        Path ddlFilePath = tempDir.resolve("t1");
        Files.writeString(ddlFilePath, generateDdl());
        Qianyi qianyi =  new Qianyi();
        qianyi.setJobId(job.getId());
        qianyi.setOkFilePath(okFilePath.toString());
        qianyi.setDdlFilePath(ddlFilePath.toString());
        qianyi.setTargetSchema("test2");
        qianyi.setTargetTableName("t1");
        qianyi.setStatus(BatchStatus.PROCESSING);
        qianyiRepository.save(qianyi);

        // 迁移明细
        Path sourceCsvPath =  tempDir.resolve("ibm1388_source1.csv");

        List<List<String>> rows = new ArrayList<>();
        Long totalDataRows = 1L;
        StringBuilder csvContent = new StringBuilder();
        for(int i=0; i < totalDataRows; i++) {
            List<String> row = generateRowData(i + 125L);
            rows.add(row);
            csvContent.append(row.stream().collect(Collectors.joining(",")));
            if (i < totalDataRows - 1) {
                csvContent.append("\n");
            }
        }
        writeWithEncoding(sourceCsvPath.toString(), csvContent.toString(), CHARSET_IBM);
        QianyiDetail d1 = new QianyiDetail();
        d1.setJobId(job.getId());
        d1.setQianyiId(qianyi.getId());
        d1.setSourceCsvPath(sourceCsvPath.toString());
        d1.setTargetSchema(qianyi.getTargetSchema());
        d1.setTargetTableName(qianyi.getTargetTableName());
        d1.setStatus(DetailStatus.TRANSCODING);
        qianyiDetailRepository.save(d1);

        transcodeService.execute(d1.getId());

        QianyiDetail qianyiDetailById = qianyiDetailRepository.findById(d1.getId()).orElseThrow();
        assertEquals(DetailStatus.PROCESSING_CHILDS, qianyiDetailById.getStatus());

        List<CsvSplit> byDetailId = csvSplitRepository.findByDetailId(qianyiDetailById.getId());
        assertEquals(1, byDetailId.size());

        // 更新为正在装载
        Long splitId = byDetailId.get(0).getId();
        csvSplitRepository.updateStatus(splitId, CsvSplitStatus.WAIT_LOAD, CsvSplitStatus.LOADING);
        loadService.execute(splitId);

        // 更新为正在验证
        csvSplitRepository.updateStatus(splitId, CsvSplitStatus.WAIT_VERIFY, CsvSplitStatus.VERIFYING);
        verifyService.execute(splitId);

        qianyiDetailById = qianyiDetailRepository.findById(d1.getId()).orElseThrow();
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

}
