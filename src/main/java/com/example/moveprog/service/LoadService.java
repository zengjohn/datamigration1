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

    private final AppProperties config;

    public void execute(Long splitId) {
        CsvSplit csvSplit = splitRepo.findById(splitId).orElse(null);
        if (null == csvSplit || csvSplit.getStatus() != CsvSplitStatus.LOADING) {
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
            loadSingleSplitFile(csvSplit, columnNames);

            // 2. 成功提交 -> 待验证
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.WAIT_VERIFY, null);
            log.info("  [Load] 装载完成 Split: {}", splitId);
        } catch (JobStoppedException e) {
            log.warn("装载任务因作业停止而中断: Split[{}]", splitId);
            // 【关键】如果是被停止的，不要标记为 FAIL！
            // 应该把状态回滚为 WAIT_LOAD，这样用户点击“恢复”后，调度器能立马再次捡起它
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.WAIT_LOAD, "人工停止，重置等待");
        } catch (Exception e) {
            log.error("  [Load] 异常 Split: {}", splitId);
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.FAIL_LOAD, e.getMessage());
            // 失败了也要尝试刷新父状态(可能变红)
            // stateManager.refreshDetailStatus... (Load失败通常不急着刷新detail，因为还没跑完，看业务需求)
        }
    }

    /**
     * 单个切分文件装载 (原子操作：删 + 插)
     * @param split
     * @param columnNames csv ddl中定义的列名
     */
    public void loadSingleSplitFile(CsvSplit split, List<String> columnNames) {
        Long detailId = split.getDetailId();
        QianyiDetail qianyiDetail = detailRepo.findById(detailId).orElseThrow();
        String tableName = qianyiDetail.getTableName();
        log.info("开始装载切分文件: tableName: {}, split id: {}", qianyiDetail.getTableName(), split.getId());

        try {
            // Step 1: 幂等删除 (根据 csvid 清理旧数据)
            targetDatabaseConnectionManager.deleteLoadOldData(qianyiDetail.getJobId(), tableName, split.getId());
        } catch (Exception e) {
            log.error("清除已经装载的数据失败: Path={}", split.getSplitFilePath(), e);
            split.setStatus(CsvSplitStatus.FAIL_LOAD);
            split.setErrorMsg(e.getMessage());
            splitRepo.save(split);
            throw new RuntimeException(e);
        }

        try {
            load(tableName, split, columnNames);

            // Step 3: 标记成功
            split.setStatus(CsvSplitStatus.WAIT_VERIFY);
            split.setErrorMsg(null);
            splitRepo.save(split);
        } catch (Exception e) {
            log.error("切分文件装载失败: Path={}", split.getSplitFilePath(), e);
            split.setStatus(CsvSplitStatus.FAIL_LOAD);
            split.setErrorMsg(e.getMessage());
            splitRepo.save(split);
            throw new RuntimeException(e);
        }
    }

    private void load(String tableName, CsvSplit split, List<String> columnNames) throws Exception {
        try (Connection conn = targetDatabaseConnectionManager.getConnection(split.getJobId(), false)) {
            executeSqlsBeforeLoad(conn);

            // 【核心判断】根据配置选择装载策略
            if (config.getLoadJdbc().isUseLocalInfile()) {
                // 方案 A: 极速 LOAD DATA (原有逻辑)
                loadByLoadDataInFile(conn, tableName, split, columnNames);
            } else {
                // 方案 B: 通用 JDBC Batch Insert (新增保底)
                loadByJdbcBatch(conn, tableName, split, columnNames);
            }
            conn.commit();
        }
    }

    /**
     * 【新增】使用 JDBC PreparedStatement 批量插入
     */
    private void loadByJdbcBatch(Connection conn, String tableName, CsvSplit split, List<String> columns) throws Exception {
        // 1. 构建 Insert SQL: INSERT INTO table (col1, col2, source_row_no) VALUES (?, ?, ?)
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");

        // 拼接列名
        for (String col : columns) {
            sql.append(col).append(",");
        }
        sql.append("source_row_no) VALUES ("); // 您的CSV最后也是 source_row_no

        // 拼接占位符
        for (int i = 0; i < columns.size(); i++) {
            sql.append("?,");
        }
        sql.append("?)"); // 对应 source_row_no

        String insertSql = sql.toString();
        log.info("Switching to JDBC Batch Insert: {}", insertSql);

        // 2. 准备 CSV 解析器
        AppProperties.CsvDetailConfig utf8Config = config.getCsv().getUtf8Split();
        CsvParserSettings settings = utf8Config.toParserSettings(); // 复用配置
        CsvParser parser = new CsvParser(settings);
        Charset charset = CharsetFactory.resolveCharset(utf8Config.getEncoding());

        int batchSize = config.getLoadJdbc().getBatchSize();
        if (batchSize <= 0) batchSize = 5000;

        try (PreparedStatement ps = conn.prepareStatement(insertSql);
             InputStreamReader reader = new InputStreamReader(new FileInputStream(split.getSplitFilePath()), charset)) {

            parser.beginParsing(reader);
            String[] row;
            long count = 0;

            while ((row = parser.parseNext()) != null) {
                // CSV行结构: [业务列1, 业务列2, ..., 行号]
                // 刚好与我们的 Insert SQL 参数顺序对应

                // 填充参数
                for (int i = 0; i < row.length; i++) {
                    // 这里简化处理，全部 setString，依赖 JDBC 驱动做类型转换
                    // 如果遇到特殊类型（如 Blob/Binary），可能需要更精细的处理
                    ps.setString(i + 1, row[i]);
                }

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

    private void loadByLoadDataInFile(Connection conn, String tableName, CsvSplit split, List<String> columnNames) throws Exception {
        // 每次操作创建独立的连接 (或者你可以引入 DruidDataSource 动态创建连接池，这里用最简单的原生JDBC演示)
        // 注意：LOAD DATA LOCAL INFILE 需要特殊的驱动设置
        // Step 2: 构造 LOAD DATA SQL
        String loadSql = generateLoadSql(tableName, split, columnNames);

        // 执行装载
        // 注意：如果是 MySQL，必须在 URL 加上 allowLoadLocalInfile=true
        // 且这里的 loadSql 可能需要转为 InputStream 方式发送，或者直接 execute
        // 简单方式：
        // boolean autoCommit = config.getLoadJdbc().isAutoCommit();
        // load data infile 数据库自己会处理事务（提交或者回滚，程序不用显式出来）
        targetDatabaseConnectionManager.executeUpdateSql(conn, loadSql);
    }


    /**
     * 动态生成 LOAD DATA SQL
     * @param tableName 目标表名
     * @param csvSplit
     */
    private String generateLoadSql(String tableName, CsvSplit csvSplit, List<String> columns) {
        String splitCsvPath = csvSplit.getSplitFilePath();
        Long splitId = csvSplit.getId();

        String safePath = splitCsvPath.replace("\\", "/");

        AppProperties.CsvDetailConfig utf8Split = config.getCsv().getUtf8Split();

        StringBuilder sb = new StringBuilder();
        sb.append("LOAD DATA LOCAL INFILE '").append(safePath).append("' ");
        sb.append("INTO TABLE ").append(tableName).append(" ");
        sb.append("CHARACTER SET utf8mb4 ");
        sb.append("FIELDS TERMINATED BY '").append(utf8Split.getDelimiter()).append("' ");
        sb.append("OPTIONALLY ENCLOSED BY '").append(utf8Split.getQuote()).append("' ");
        sb.append("LINES TERMINATED BY '").append(utf8Split.getLineSeparator()).append("' ");

        // --- 关键部分：列映射 ---
        // CSV 文件的结构是: [DDL列1, DDL列2, ... DDL列N, 行号]
        // SQL 语法: (col1, col2, ..., @var_lineno)
        
        sb.append("(");
        // 1. 拼接DDL中的业务列名
        for (String col : columns) {
            sb.append(col).append(", ");
        }
        sb.append("source_row_no");
        sb.append(")");
        // -- 【关键】在这里把当前的 csvid 赋值给每一行
        sb.append(" SET csvid=").append(splitId);

        return sb.toString();
    }

}