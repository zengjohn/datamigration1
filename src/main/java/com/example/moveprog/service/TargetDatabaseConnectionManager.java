package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Service
@Slf4j
@RequiredArgsConstructor
public class TargetDatabaseConnectionManager {
    private final QianyiDetailRepository detailRepo;
    private final MigrationJobRepository jobRepo;
    private final AppProperties config;

    public Connection getConnection(Long detailId) throws SQLException {
        QianyiDetail qianyiDetail = detailRepo.findById(detailId).orElseThrow();
        MigrationJob migrationJob = jobRepo.findById(qianyiDetail.getJobId()).orElseThrow();

        // 准备数据库连接信息
        String url = migrationJob.getTargetDbUrl() + config.getLoadJdbc().getUrlOptions();
        String user = migrationJob.getTargetDbUser();
        String password = migrationJob.getTargetDbPass();

        return DriverManager.getConnection(url, user, password);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void deleteLoadOldData(String tableName, Long detailId, Long splitId) throws SQLException {
        try (Connection conn = getConnection(detailId)) {
            String deleteSql = "DELETE FROM " + tableName + " WHERE csvid=" + splitId;
            executeUpdateSql(conn, deleteSql);
        }
    }

    public static void executeUpdateSql(Connection conn, String sql) throws SQLException {
        try(Statement stmt = conn.createStatement()) {
            log.info("sql：{}", sql);
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            log.error("failed to execute {}", sql);
            throw e;
        }
    }

}
