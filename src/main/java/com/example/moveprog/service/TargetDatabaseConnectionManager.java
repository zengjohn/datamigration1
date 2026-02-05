package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.repository.CsvSplitRepository;
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class TargetDatabaseConnectionManager {
    private final MigrationJobRepository jobRepo;
    private final CsvSplitRepository splitRepo;
    private final QianyiDetailRepository qianyiDetailRepo;
    private final JdbcHelper jdbcHelper;
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

        // 2. 从配置文件查 调优参数
        AppProperties.TargetDbConfig template = appProperties.getTargetDbConfig();

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
        config.setPoolName("HikariPool-Job-" + jobId);

        // --- 模板部分 (来自 yml) ---
        config.setMaximumPoolSize(template.getMaxPoolSize());
        config.setMinimumIdle(template.getMinIdle());
        config.setConnectionTimeout(template.getConnectionTimeout());
        config.setAutoCommit(template.isAutoCommit());

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

    /**
     * 用于重新装载
     * @param splitId
     * @throws SQLException
     * @throws IOException
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void deleteLoadOldData(Long splitId) throws SQLException, IOException {
        CsvSplit csvSplit = splitRepo.findById(splitId).orElseThrow();
        MigrationJob migrationJob = jobRepo.findById(csvSplit.getJobId()).orElseThrow();
        try (Connection conn = getConnection(migrationJob.getId(), false)) {
            String deleteSql = jdbcHelper.deleteSql(splitId);
            executeUpdateSql(conn, deleteSql);
        }
    }

    /**
     * 用于重新转码
     * @param detailId
     * @throws SQLException
     * @throws IOException
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void deleteSplitsAndLoadData(Long detailId) throws SQLException, IOException {
        QianyiDetail qianyiDetail = qianyiDetailRepo.findById(detailId).orElseThrow();
        MigrationJob migrationJob = jobRepo.findById(qianyiDetail.getJobId()).orElseThrow();

        List<CsvSplit> csvSplits = splitRepo.findByDetailId(detailId);
        if (Objects.nonNull(csvSplits) && !csvSplits.isEmpty()) {
            try (Connection conn = getConnection(migrationJob.getId(), false)) {
                for (CsvSplit csvSplit : csvSplits) {
                    String deleteSql = jdbcHelper.deleteSql(csvSplit.getId());
                    executeUpdateSql(conn, deleteSql);
                }
            }
        }

        // DELETE FROM t_csv_split WHERE detail_id = ?
        splitRepo.deleteByDetailId(detailId);
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
