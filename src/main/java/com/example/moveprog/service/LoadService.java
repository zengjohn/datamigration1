package com.example.moveprog.service;

import com.example.moveprog.entity.*;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class LoadService {

    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;
    private final QianyiRepository qianyiRepo;
    private final MigrationJobRepository jobRepo;
    private final TaskLockManager lockManager;

    // 必须与 ThreadPoolConfig 中的方法名一致，或者使用 @Qualifier("dbLoadExecutor")
    @Qualifier("dbLoadExecutor")
    private final Executor dbLoadExecutor;

    @Async
    public void execute(Long detailId) {
        // 1. 【进门加锁】
        if (!lockManager.tryLock(detailId)) {
            if (log.isDebugEnabled()) {
                log.debug("装载任务正在运行中，跳过: {}", detailId);
            }
            return;
        }

        try {
            QianyiDetail detail = detailRepo.findById(detailId).orElse(null);
            if (detail == null ||
                    !(detail.getStatus() == DetailStatus.WAIT_LOAD || detail.getStatus() == DetailStatus.WAIT_RELOAD)) {
                return;
            }

            // 0. 更新状态为正在装载
            detail.setStatus(DetailStatus.LOADING);
            detailRepo.save(detail);

            long failLoadCount = splitRepo.countByDetailIdAndStatusNot(detailId, CsvSplitStatus.WAIT_VERIFY);
            if (0 == failLoadCount) {
                log.info("任务[{}] 没有 待装载切分 文件，直接完成", detailId);
                finishDetailLoad(detail);
                return;
            }

            // 1. 获取上下文信息
            Qianyi batch = qianyiRepo.findById(detail.getQianyiId()).orElseThrow(() -> new RuntimeException("批次不存在"));

            // 2. 解析 DDL 获取列名列表 (用于构造 LOAD DATA 语句)
            // 假设 DDL 文件路径存在 batch.getDdlFilePath() 中
            List<String> columnNames = SchemaParseUtil.parseColumnNamesFromDdl(batch.getDdlFilePath());

            // 3. 获取待装载切分文件 (待装载)
            List<CsvSplit> splits = splitRepo.findByDetailIdAndStatus(detailId, CsvSplitStatus.WAIT_LOAD);
            if (!splits.isEmpty()) {
                log.info("任务[{}] 开始并发装载 {} 个文件, 表: {}", detailId, splits.size(), batch.getTableName());

                MigrationJob job = jobRepo.findById(batch.getJobId())
                        .orElseThrow(() -> new RuntimeException("作业配置不存在"));
                // 准备数据库连接信息
                String url = job.getTargetJdbcUrl();
                String user = job.getTargetUser();
                String password = job.getTargetPassword();

                // 4. 并发装载
                List<CompletableFuture<Void>> futures = splits.stream()
                        .map(split -> CompletableFuture.runAsync(() -> {
                            // 调用原子装载方法
                            // 传入连接信息，而不是用注入的 template
                            loadSingleSplitFile(batch.getTableName(), split, columnNames, url, user, password);
                        }, dbLoadExecutor))
                        .collect(Collectors.toList());

                // 等待所有任务结束
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            }

            // 5. 检查最终结果
            long failLoadCount1 = splitRepo.countByDetailIdAndStatusNot(detailId, CsvSplitStatus.WAIT_VERIFY);
            if (failLoadCount1 == 0) {
                finishDetailLoad(detail);
                log.info("装载成功 detail id: {}, 源文件: {}", detail.getId(), detail.getSourceCsvPath());
            } else {
                detail.setStatus(DetailStatus.WAIT_RELOAD); // 等待重试
                detail.setErrorMsg("有 " + failLoadCount1 + " 个切分文件装载失败");
                detailRepo.save(detail);
            }
        } catch (Exception e) {
            QianyiDetail detail = detailRepo.findById(detailId).orElse(null);
            if (null != detail) {
                log.error("装载流程异常 detail id: {}, 源文件: {}", detail.getId(), detail.getSourceCsvPath(), e);
                detail.setStatus(DetailStatus.FAIL_LOAD);
                detail.setErrorMsg(e.getMessage());
                detailRepo.save(detail);
            } else {
                log.error("装载流程异常 TaskId={}", detailId, e);
            }
        } finally {
            // 2. 【出门解锁】
            lockManager.releaseLock(detailId);
        }
    }

    /**
     * 单个切分文件装载 (原子操作：删 + 插)
     * @param tableName
     * @param split
     * @param columnNames csv ddl中定义的列名
     */
    @Transactional(rollbackFor = Exception.class)
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
            throw new RuntimeException(e); // 抛出异常以便 CompletableFuture 感知
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

    private void finishDetailLoad(QianyiDetail detail) {
        // 可以设置为 WAIT_VERIFY 等待后续校验，或者直接 PASS
        detail.setStatus(DetailStatus.WAIT_VERIFY); 
        detail.setProgress(100);
        detail.setErrorMsg(null);
        detailRepo.save(detail);
        log.info("源文件任务[{}] 装载完成", detail.getId());
    }
}