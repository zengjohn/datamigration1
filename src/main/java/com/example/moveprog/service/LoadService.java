package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.*;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.exception.JobStoppedException;
import com.example.moveprog.repository.*;
import com.example.moveprog.util.CharsetFactory;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * 装载服务
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class LoadService {
    private final StateManager stateManager;

    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;
    private final QianyiRepository qianyiRepo;

    private final JobControlManager jobControlManager;
    private final TargetDatabaseConnectionManager targetDatabaseConnectionManager;
    private final JdbcHelper jdbcHelper;
    private final MigrationArtifactManager migrationArtifactManager;

    private final AppProperties config;

    public void execute(Long splitId) {
        CsvSplit csvSplit = splitRepo.findById(splitId).orElse(null);
        if (null == csvSplit || csvSplit.getStatus() != CsvSplitStatus.LOADING) {
            // 如果已经是 LOADING，说明可能被其他线程抢了，或者状态不对，直接退出
            return;
        }

        log.info("  [Load] 开始装载 Split id: {}, detailId: {}, csv: {}", splitId, csvSplit.getDetailId(), csvSplit.getSplitFilePath());

        try {
            // 【埋点1】刚进来先查一下
            jobControlManager.checkJobState(csvSplit.getJobId()); // 如果抛异常，直接被下面捕获

            Qianyi qianyi = qianyiRepo.findById(csvSplit.getQianyiId()).orElseThrow();

            // 2. 解析 DDL 获取列名列表 (用于构造 LOAD DATA 语句)
            // 假设 DDL 文件路径存在 batch.getDdlFilePath() 中
            List<String> columnNames = SchemaParseUtil.parseColumnNamesFromDdl(qianyi.getDdlFilePath());

            // 3. 获取待装载切分文件 (待装载)
            loadSingleSplitFileWithRetry(csvSplit, columnNames);

            // 2. 成功提交 -> 待验证
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.WAIT_VERIFY, "装载完成");
            log.info("  [Load] 装载完成 Split: {}", splitId);
        } catch (JobStoppedException e) {
            log.warn("装载任务因作业停止而中断: Split[{}]", splitId);
            // 【关键】如果是被停止的，不要标记为 FAIL！
            // 应该把状态回滚为 WAIT_LOAD，这样用户点击“恢复”后，调度器能立马再次捡起它
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.WAIT_LOAD, "人工停止，重置等待");
        } catch (Exception e) {
            log.error("  [Load] 异常 Split: {}", splitId);
            // 只有重试耗尽后，才标记为 FAIL
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.FAIL_LOAD, "重试耗尽: " +e.getMessage());
            // 失败了也要尝试刷新父状态(可能变红)
            // stateManager.refreshDetailStatus... (Load失败通常不急着刷新detail，因为还没跑完，看业务需求)
        }
    }

    private void loadSingleSplitFileWithRetry(CsvSplit split, List<String> columnNames) throws Exception {
        int maxRetries = config.getLoadJdbc().getMaxRetries();
        if (maxRetries <= 0) maxRetries = 1;

        int retryCount = 0;
        Exception lastException = null;

        // 【核心】重试循环
        while (retryCount < maxRetries) {
            try {
                if (retryCount > 0) {
                    log.info("切片[{}] 第 {} 次重试装载...", split.getId(), retryCount + 1);
                    // 避让策略：指数退避 (2s, 4s, 8s...) 防止雪崩
                    Thread.sleep(2000L * (long) Math.pow(2, retryCount - 1));
                }

                // --- 您的核心装载逻辑 (保留您原有的逻辑) ---
                loadSingleSplitFile(split);
                // ---------------------------------------

                // 如果运行到这里没有抛异常，说明成功了，直接返回
                return;

            } catch (Exception e) {
                lastException = e;
                retryCount++;
                log.warn("切片[{}] 装载异常 (第{}/{}次): {}", split.getId(), retryCount + 1, maxRetries, lastException.getMessage());

                // 【关键】重试前必须清理“半成品”数据
                // 既然 loadSingleSplitFile 失败了，事务可能回滚了，也可能因为网络原因状态未知
                // 所以必须显式清理一次，保证下次重试是干净的
                try {
                    targetDatabaseConnectionManager.deleteLoadOldData(split.getId());
                } catch (Exception cleanupEx) {
                    log.error("重试前清理脏数据失败", cleanupEx);
                }
            }
        }

        // 循环结束仍未返回，说明失败
        throw lastException;

    }

    /**
     * 单个切分文件装载 (原子操作：删 + 插)
     * 单次装载逻辑 (不要在这里更新 DB 状态，只管抛异常)
     * @param split
     */
    private void loadSingleSplitFile(CsvSplit split) throws Exception {
        // Step 1: 幂等删除 (根据 csvid 清理旧数据)
        migrationArtifactManager.cleanVerifyArtifacts(split);
        targetDatabaseConnectionManager.deleteLoadOldData(split.getId());

        try (Connection conn = targetDatabaseConnectionManager.getConnection(split.getJobId(), false)) {
            executeSqlsBeforeLoad(conn);

            // 【核心判断】根据配置选择装载策略
            if (config.getLoadJdbc().isUseLocalInfile()) {
                // 方案 A: 极速 LOAD DATA (原有逻辑)
                loadByLoadDataInFile(conn, split.getId());
            } else {
                // 方案 B: 通用 JDBC Batch Insert (新增保底)
                loadByJdbcBatch(conn, split.getId());
            }
            conn.commit();
        }
    }


    /**
     * 【新增】使用 JDBC PreparedStatement 批量插入
     */
    private void loadByJdbcBatch(Connection conn, Long splitId) throws Exception {

        Pair<String,List<String>> loadJdbcSqlPair = jdbcHelper.loadJdbcSql(splitId);
        String sql = loadJdbcSqlPair.getLeft();
        List<String> loadSqlColumns = loadJdbcSqlPair.getRight();

        log.info("using JDBC Batch Insert: {}", sql);

        CsvSplit csvSplit = splitRepo.findById(splitId).orElseThrow();

        // 2. 准备 CSV 解析器
        AppProperties.CsvDetailConfig utf8Config = config.getCsv().getUtf8Split();
        CsvParserSettings settings = utf8Config.toParserSettings(); // 复用配置
        CsvParser parser = new CsvParser(settings);
        Charset charset = CharsetFactory.resolveCharset(utf8Config.getEncoding());

        int batchSize = config.getLoadJdbc().getBatchSize();
        if (batchSize <= 0) batchSize = 5000;

        try (PreparedStatement ps = conn.prepareStatement(sql);
             InputStreamReader reader = new InputStreamReader(new FileInputStream(csvSplit.getSplitFilePath()), charset)) {
            // 【关键】设置超时，防止死锁或网络丢包导致线程永久挂起
            // 建议在 application.yml 里配置 load-jdbc.query-timeout: 600
            int timeout = config.getLoadJdbc().getQueryTimeout();
            if (timeout > 0) {
                ps.setQueryTimeout(timeout);
            }

            parser.beginParsing(reader);
            String[] row;
            long count = 0;

            while ((row = parser.parseNext()) != null) {
                // CSV行结构: [业务列1, 业务列2, ..., csv拆分id, 行号]
                // 刚好与我们的 Insert SQL 参数顺序对应

                // 填充参数
                for (int i = 0; i < row.length-1; i++) {
                    // 这里简化处理，全部 setString，依赖 JDBC 驱动做类型转换
                    // 如果遇到特殊类型（如 Blob/Binary），可能需要更精细的处理
                    ps.setString(i + 1, row[i]);
                }
                ps.setLong(loadSqlColumns.size()-1, csvSplit.getId());
                ps.setLong(loadSqlColumns.size(), Long.parseLong(row[row.length-1])); // ibm csv中行号

                ps.addBatch();

                if (++count % batchSize == 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                }
            }
            // 提交剩余的
            ps.executeBatch();
            ps.clearBatch();
        }
    }

    // 【新增优化】 会话级加速配置, 比如执行
    // SET unique_checks=0; SET foreign_key_checks=0;
    // 如果账号有权限且不需要Binlog, SET sql_log_bin=0;
    private void executeSqlsBeforeLoad(Connection conn) throws SQLException {
        List<String> preSqlList = config.getLoadJdbc().getPreSqlList();
        if (preSqlList != null && !preSqlList.isEmpty()) {
            try(Statement stmt = conn.createStatement()) {
                for (String sqlStr : preSqlList) {
                    stmt.execute(sqlStr);
                }
            }
        }
    }

    private void loadByLoadDataInFile(Connection conn, Long splitId) throws Exception {
        // 每次操作创建独立的连接 (或者你可以引入 DruidDataSource 动态创建连接池，这里用最简单的原生JDBC演示)
        // 注意：LOAD DATA LOCAL INFILE 需要特殊的驱动设置
        // Step 2: 构造 LOAD DATA SQL
        String loadSql = jdbcHelper.loadDataInfileSql(splitId);

        // 执行装载
        // 注意：如果是 MySQL，必须在 URL 加上 allowLoadLocalInfile=true
        // 且这里的 loadSql 可能需要转为 InputStream 方式发送，或者直接 execute
        // 简单方式：
        // boolean autoCommit = config.getLoadJdbc().isAutoCommit();
        // load data infile 数据库自己会处理事务（提交或者回滚，程序不用显式出来）
        targetDatabaseConnectionManager.executeUpdateSql(conn, loadSql);
    }



}