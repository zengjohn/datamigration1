package com.example.moveprog.service;

import com.example.moveprog.entity.*;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
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
    private final MigrationJobRepository jobRepo;

    public void execute(Long splitId) {
        // 1. 抢占任务 (乐观锁)
        if (!stateManager.switchSplitStatus(splitId, CsvSplitStatus.LOADING, null)) {
            return;
        }

        try {
            log.info("  [Load] 开始装载 Split: {}", splitId);

            CsvSplit csvSplit = splitRepo.findById(splitId).orElse(null);
            QianyiDetail detailById = detailRepo.findById(csvSplit.getDetailId()).orElseThrow();
            Qianyi qianyi = qianyiRepo.findById(csvSplit.getQianyiId()).orElseThrow();

            // 2. 解析 DDL 获取列名列表 (用于构造 LOAD DATA 语句)
            // 假设 DDL 文件路径存在 batch.getDdlFilePath() 中
            List<String> columnNames = SchemaParseUtil.parseColumnNamesFromDdl(qianyi.getDdlFilePath());

            // 3. 获取待装载切分文件 (待装载)
            MigrationJob job = jobRepo.findById(csvSplit.getJobId())
                    .orElseThrow(() -> new RuntimeException("作业配置不存在"));
            // 准备数据库连接信息
            String url = job.getTargetJdbcUrl();
            String user = job.getTargetUser();
            String password = job.getTargetPassword();

            loadSingleSplitFile(detailById.getTableName(), csvSplit, columnNames, url, user, password);

            // 2. 成功提交 -> 待验证
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.WAIT_VERIFY, null);
            log.info("  [Load] 装载完成 Split: {}", splitId);
        } catch (Exception e) {
            log.error("  [Load] 异常 Split: {}", splitId);
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.FAIL_LOAD, e.getMessage());
            // 失败了也要尝试刷新父状态(可能变红)
            // stateManager.refreshDetailStatus... (Load失败通常不急着刷新detail，因为还没跑完，看业务需求)
        }
    }

    /**
     * 单个切分文件装载 (原子操作：删 + 插)
     * @param tableName
     * @param split
     * @param columnNames csv ddl中定义的列名
     */
    public void loadSingleSplitFile(String tableName, CsvSplit split, List<String> columnNames, String url, String user, String pwd) {
        log.info("开始装载切分文件: tableName: {}, split id: {}", tableName, split.getId());

        try (Connection conn = DriverManager.getConnection(url, user, pwd)) {
            // 每次操作创建独立的连接 (或者你可以引入 DruidDataSource 动态创建连接池，这里用最简单的原生JDBC演示)
            // 注意：LOAD DATA LOCAL INFILE 需要特殊的驱动设置
            // Step 1: 幂等删除 (根据 csvid 清理旧数据)
            String deleteSql = "DELETE FROM " + tableName + " WHERE csvid=" + split.getId();
            executeUpdateSql(conn, deleteSql);

            // Step 2: 构造 LOAD DATA SQL
            String loadSql = generateLoadSql(tableName, split, columnNames);

            // 执行装载
            // 注意：如果是 MySQL，必须在 URL 加上 allowLoadLocalInfile=true
            // 且这里的 loadSql 可能需要转为 InputStream 方式发送，或者直接 execute
            // 简单方式：
            executeUpdateSql(conn, loadSql);

            // Step 3: 标记成功
            split.setStatus(CsvSplitStatus.WAIT_VERIFY);
            split.setErrorMsg(null);
            splitRepo.save(split);
        } catch (Exception e) {
            log.error("切分文件装载失败: Path={}", split.getSplitFilePath(), e);
            split.setStatus(CsvSplitStatus.FAIL_LOAD);
            split.setErrorMsg(e.getMessage());
            splitRepo.save(split);
            throw new RuntimeException(e); // 抛出异常以便 CompletableFuture 感知, 以及回滚事务
        }
    }

    private void executeUpdateSql(Connection conn, String sql) throws SQLException {
        try(Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            log.error("failed to execute {}", sql);
            throw e;
        }
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

        StringBuilder sb = new StringBuilder();
        sb.append("LOAD DATA LOCAL INFILE '").append(safePath).append("' ");
        sb.append("INTO TABLE ").append(tableName).append(" ");
        sb.append("CHARACTER SET utf8mb4 ");
        sb.append("FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' ");
        
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
        sb.append("SET csvid=").append(splitId);

        return sb.toString();
    }

}