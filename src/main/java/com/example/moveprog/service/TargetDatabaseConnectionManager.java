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
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
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

    // 【新增】注入 MeterRegistry 用于监控注册
    @Autowired
    private MeterRegistry meterRegistry;

    // 【核心】缓存：JobId -> 连接池
    // 使用 ConcurrentHashMap 保证并发安全
    private final Map<Long, HikariDataSource> dataSourceCache = new ConcurrentHashMap<>();

    /**
     * 获取指定作业的数据库连接
     * 注意：目标库的连接池是手工管理的， spring管理不了目标库的事务
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

        String url;
        if (migrationJob.getTargetUrl() != null) {
            url = migrationJob.getTargetUrl() + appProperties.getJdbcOptions();
        } else {
            url = "jdbc:mysql://" + migrationJob.getTargetDbHost() + ":" + migrationJob.getTargetDbPort() + "/" + migrationJob.getTargetSchema() + appProperties.getJdbcOptions();
        }
        String user = migrationJob.getTargetDbUser();
        String password = migrationJob.getTargetDbPass();

        // 【自动补全性能参数】
        if (!url.contains("rewriteBatchedStatements")) {
            if (url.indexOf('?') == -1) {
                // 没有问号，直接加 ?key=value
                url += "?rewriteBatchedStatements=true";
            } else {
                // 有问号
                if (url.endsWith("?") || url.endsWith("&")) {
                    // 结尾已经是分隔符，直接加
                    url += "rewriteBatchedStatements=true";
                } else {
                    // 结尾是参数值，加 &key=value
                    url += "&rewriteBatchedStatements=true";
                }
            }
        }
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        // 关键点：给每个连接池起个唯一名字，方便监控区分
        config.setPoolName("HikariPool-Job-" + jobId);
        // 【核心修改】将连接池注册到 Micrometer，这样 Actuator 才能看到它
        config.setMetricRegistry(meterRegistry);

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
     * 注意：事务管理只适用于元数据库，目标库由于连接
     * @param splitId
     * @throws SQLException
     * @throws IOException
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void deleteLoadOldData(Long splitId) throws SQLException, IOException {
        CsvSplit csvSplit = splitRepo.findById(splitId).orElseThrow();
        MigrationJob migrationJob = jobRepo.findById(csvSplit.getJobId()).orElseThrow();
        try (Connection conn = getConnection(migrationJob.getId(), false)) {
            // 1. 显式关闭自动提交 (防御性编程)
            conn.setAutoCommit(false);

            try {
                Pair<String, List<Object>> sqlPair = jdbcHelper.deleteSql(splitId);
                executeUpdate(conn, sqlPair.getKey(), sqlPair.getValue());

                // 2. 【必需】显式提交
                conn.commit();
            } catch (Exception e) {
                // 3. 出错回滚
                if (conn != null) {
                    try {
                        conn.rollback();
                    } catch (SQLException ex) {
                        log.error("Rollback failed", ex);
                    }
                }
                throw e;
            }
        }
    }

    /**
     * 删除拆分（元数据）和目标库已经装载数据, 用于重新转码
     * 注意：事务管理只适用于元数据库，目标库由于连接
     * @param detailId
     * @throws SQLException
     * @throws IOException
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void deleteSplitsAndLoadData(Long detailId) throws SQLException {
        QianyiDetail qianyiDetail = qianyiDetailRepo.findById(detailId).orElseThrow();
        MigrationJob migrationJob = jobRepo.findById(qianyiDetail.getJobId()).orElseThrow();

        List<CsvSplit> csvSplits = splitRepo.findByDetailId(detailId);
        if (Objects.nonNull(csvSplits) && !csvSplits.isEmpty()) {
            try (Connection conn = getConnection(migrationJob.getId(), false)) {
                // 1. 显式关闭自动提交 (防御性编程，不管全局配置怎样，这里强制手动挡)
                conn.setAutoCommit(false);
                try {
                    for (CsvSplit csvSplit : csvSplits) {
                        // 2. 执行多条 SQL
                        Pair<String, List<Object>> deleteSqlPair = jdbcHelper.deleteSql(csvSplit.getId());
                        executeUpdate(conn, deleteSqlPair.getKey(), deleteSqlPair.getValue());
                    }
                    // 3. 【必须】显式提交
                    conn.commit();
                } catch (Exception e) {
                    // 4. 出错回滚
                    if (conn != null) {
                        try {
                            conn.rollback();
                        } catch (SQLException ex) {
                            log.error("Rollback failed", ex);
                        }
                    }
                    throw e;
                }
            }
        }
    }

    /**
     * 【新增】支持参数绑定的执行更新方法
     */
    public static void executeUpdate(Connection conn, String sql, List<Object> params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (params != null) {
                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }
            }
            log.info("Execute update SQL: {} with params: {}", sql, params);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to execute SQL: {} with params: {}", sql, params, e);
            throw e;
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
