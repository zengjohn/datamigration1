package com.example.moveprog.scheduler;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.dto.OkFileContent;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.BatchStatus;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.example.moveprog.repository.QianyiRepository;
import com.example.moveprog.service.JdbcHelper;
import com.example.moveprog.service.TargetDatabaseConnectionManager;
import com.google.gson.Gson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DirectoryMonitorTest {

    @InjectMocks
    private DirectoryMonitor directoryMonitor;

    @Mock private MigrationJobRepository jobRepo;
    @Mock private QianyiRepository qianyiRepo;
    @Mock private QianyiDetailRepository detailRepo;
    @Mock private JdbcHelper jdbcHelper;
    @Mock private TargetDatabaseConnectionManager targetDatabaseConnectionManager;
    @Mock private AppProperties config;
    @Mock private Connection connection;
    @Mock private Statement statement;
    @Mock private ResultSet resultSet;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        // leniency for common mocks
    }

    @Test
    @DisplayName("测试正常解析OK文件")
    void testProcessOkFile_Normal() throws Exception {
        // 1. 准备数据
        String ip = "127.0.0.1";
        Long jobId = 1L;
        String jobSourceDir = tempDir.toString();
        
        // 准备文件
        Path ddlFile = tempDir.resolve("users.sql");
        Files.writeString(ddlFile, "CREATE TABLE users ...");
        
        Path csvFile = tempDir.resolve("data.csv");
        Files.writeString(csvFile, "1,john");
        
        Path okFile = tempDir.resolve("task.ok");
        OkFileContent content = new OkFileContent();
        content.ddl = "users.sql";
        content.csv = Collections.singletonList("data.csv");
        content.table = "users_target";
        Files.writeString(okFile, new Gson().toJson(content));

        MigrationJob job = new MigrationJob();
        job.setId(jobId);
        job.setSourceDirectory(jobSourceDir);
        job.setTargetSchema("db_test");

        // 2. Mock 行为
        when(config.getCurrentNodeIp()).thenReturn(ip);
        when(qianyiRepo.existsByOkFilePathAndNodeId(anyString(), anyString())).thenReturn(false);
        when(jdbcHelper.checkExistsTableSql(anyString(), anyString())).thenReturn("SELECT COUNT(*) ...");
        
        // Mock DB check
        when(targetDatabaseConnectionManager.getConnection(eq(jobId), anyBoolean())).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenReturn(1L);

        // 3. 执行
        directoryMonitor.processOkFile(job, okFile.toFile());

        // 4. 验证
        verify(qianyiRepo).save(argThat(qianyi -> 
            qianyi.getStatus() == BatchStatus.PROCESSING &&
            qianyi.getTargetTableName().equals("users_target") &&
            qianyi.getNodeId().equals(ip)
        ));
        
        verify(detailRepo).save(argThat(detail -> 
            detail.getSourceCsvPath().equals(csvFile.toAbsolutePath().toString()) &&
            detail.getNodeId().equals(ip)
        ));
    }
}