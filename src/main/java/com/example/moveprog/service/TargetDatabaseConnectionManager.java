package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class TargetDatabaseConnectionManager {
    private final QianyiDetailRepository detailRepo;
    private final MigrationJobRepository jobRepo;
    private final AppProperties appProperties;

    // 【核心】缓存：JobId -> 连接池
    // 使用 ConcurrentHashMap 保证并发安全
    private final Map<Long, HikariDataSource> dataSourceCache = new ConcurrentHashMap<>();

    /**
     * 获取指定作业的数据库连接
     */
    public Connection getConnection(Long jobId, boolean readOnly) throws SQLException {
        // computeIfAbsent: 这是一个原子操作
        // 如果缓存里有，直接返回；如果缓存里没有，执行后面的 Lambda 创建一个新的
        HikariDataSource ds = dataSourceCache.computeIfAbsent(jobId, this::createDataSource);
        Connection conn = ds.getConnection();
        // 【可选优化】显式告诉数据库这是否读操作
        // 这有助于 TDSQL Proxy 做读写分离路由（如果有的话），也能避免误修改
        conn.setReadOnly(readOnly);
        return conn;
    }

    /**
     * 创建连接池的逻辑 (Lazy Load)
     */
    private HikariDataSource createDataSource(Long jobId) {
        log.info("初始化目标库连接池, JobId={}", jobId);

        // 1. 查库获取配置
        MigrationJob migrationJob = jobRepo.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));

        // 2. 配置 HikariCP
        HikariConfig config = new HikariConfig();

        String url = migrationJob.getTargetDbUrl() + appProperties.getJdbcOptions();
        String user = migrationJob.getTargetDbUser();
        String password = migrationJob.getTargetDbPass();

        // 【自动补全性能参数】
        if (!url.contains("rewriteBatchedStatements")) {
            url += (url.contains("?") ? "&" : "?") + "rewriteBatchedStatements=true";
        }
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);

        // 3. 连接池参数 (可以从 AppProperties 读取全局默认值，也可以硬编码)
        config.setMaximumPoolSize(60); // 配合您的 128C 机器
        config.setMinimumIdle(10);
        config.setAutoCommit(false);   // 批处理必须 false
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setPoolName("HikariPool-Job-" + jobId);

        return new HikariDataSource(config);
    }

    /**
     * 【重要】应用关闭时，或者作业删除时，清理资源
     */
    @PreDestroy
    public void closeAll() {
        dataSourceCache.values().forEach(HikariDataSource::close);
        dataSourceCache.clear();
    }

    // 如果您有删除作业的功能，记得调用这个方法清理连接池
    public void invalidateJob(Long jobId) {
        HikariDataSource ds = dataSourceCache.remove(jobId);
        if (ds != null) {
            ds.close();
            log.info("已关闭并移除 JobId={} 的连接池", jobId);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void deleteLoadOldData(Long jobId, String tableName, Long splitId) throws SQLException {
        MigrationJob migrationJob = jobRepo.findById(jobId).orElseThrow();
        try (Connection conn = getConnection(migrationJob.getId(), false)) {
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
