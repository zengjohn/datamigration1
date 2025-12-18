package com.example.moveprog.service;

import com.example.moveprog.entity.*;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.repository.*;
import com.example.moveprog.scheduler.TaskLockManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
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
    private final JdbcTemplate jdbcTemplate;

    // 必须与 ThreadPoolConfig 中的方法名一致，或者使用 @Qualifier("dbLoadExecutor")
    private final Executor dbLoadExecutor;

    @Async
    public void execute(Long detailId) {
        QianyiDetail detail = detailRepo.findById(detailId).orElse(null);
        if (detail == null) {
            lockManager.releaseLock(detailId);
            return;
        }

        try {
            // 0. 更新状态为正在装载
            detail.setStatus(DetailStatus.LOADING);
            detailRepo.save(detail);

            // 1. 获取上下文信息
            Qianyi batch = qianyiRepo.findById(detail.getQianyiId())
                    .orElseThrow(() -> new RuntimeException("批次不存在"));
            MigrationJob job = jobRepo.findById(batch.getJobId())
                    .orElseThrow(() -> new RuntimeException("作业配置不存在"));

            // 2. 解析 DDL 获取列名列表 (用于构造 LOAD DATA 语句)
            // 假设 DDL 文件路径存在 batch.getDdlFilePath() 中
            List<String> columnNames = parseColumnNamesFromDdl(batch.getDdlFilePath());

            // 3. 获取待装载切分文件 (排除已成功的)
            List<CsvSplit> splits = splitRepo.findByDetailIdAndStatusNot(detailId, CsvSplitStatus.PASS);

            if (splits.isEmpty()) {
                log.info("任务[{}] 无待装载切分文件，直接完成", detailId);
                finishDetail(detail);
                return;
            }

            log.info("任务[{}] 开始并发装载 {} 个文件, 表: {}", detailId, splits.size(), batch.getTableName());

            // 4. 并发装载
            List<CompletableFuture<Void>> futures = splits.stream()
                    .map(split -> CompletableFuture.runAsync(() -> {
                        // 调用原子装载方法
                        loadSingleSplitFile(split, batch.getTableName(), columnNames);
                    }, dbLoadExecutor))
                    .collect(Collectors.toList());

            // 等待所有任务结束
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // 5. 检查最终结果
            long failCount = splitRepo.countByDetailIdAndStatusNot(detailId, CsvSplitStatus.PASS);
            if (failCount == 0) {
                finishDetail(detail);
            } else {
                detail.setStatus(DetailStatus.WAIT_RELOAD); // 等待重试
                detail.setErrorMsg("有 " + failCount + " 个切分文件装载失败");
                detailRepo.save(detail);
            }

        } catch (Exception e) {
            log.error("装载流程异常 TaskId={}", detailId, e);
            detail.setStatus(DetailStatus.FAIL_LOAD);
            detail.setErrorMsg(e.getMessage());
            detailRepo.save(detail);
        } finally {
            lockManager.releaseLock(detailId);
        }
    }

    /**
     * 单个切分文件装载 (原子操作：删 + 插)
     */
    @Transactional(rollbackFor = Exception.class)
    public void loadSingleSplitFile(CsvSplit split, String tableName, List<String> columnNames) {
        log.info("开始装载切分文件: ID={}", split.getId());

        try {
            // Step 1: 幂等删除 (根据 csvid 清理旧数据)
            String deleteSql = String.format("DELETE FROM %s WHERE csvid = ?", tableName);
            jdbcTemplate.update(deleteSql, split.getId());

            // Step 2: 构造 LOAD DATA SQL
            String loadSql = generateLoadSql(tableName, split.getSplitFilePath(), split.getId(), columnNames);
            
            // 执行装载
            jdbcTemplate.execute(loadSql);

            // Step 3: 标记成功
            split.setStatus(CsvSplitStatus.PASS);
            split.setErrorMsg(null);
            splitRepo.save(split);

        } catch (Exception e) {
            log.error("切分文件装载失败: Path={}", split.getSplitFilePath(), e);
            split.setStatus(CsvSplitStatus.FAIL);
            split.setErrorMsg(e.getMessage());
            splitRepo.save(split);
            throw new RuntimeException(e); // 抛出异常以便 CompletableFuture 感知
        }
    }

    /**
     * 解析 DDL CSV 文件获取列名
     * 格式: 列名, 类型
     */
    private List<String> parseColumnNamesFromDdl(String ddlPath) throws IOException {
        List<String> columns = new ArrayList<>();
        // 使用简单的 BufferedReader 读取，因为格式很简单
        try (BufferedReader br = Files.newBufferedReader(Paths.get(ddlPath), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                
                // 按逗号分割，取第一列
                // 注意：如果列名本身包含逗号，这里需要更复杂的解析，但通常数据库列名不会有逗号
                String[] parts = line.split(","); 
                if (parts.length >= 1) {
                    columns.add(parts[0].trim());
                }
            }
        }
        return columns;
    }

    /**
     * 动态生成 LOAD DATA SQL
     */
    private String generateLoadSql(String tableName, String csvPath, Long splitId, List<String> columns) {
        String safePath = csvPath.replace("\\", "/");

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
        // 2. 最后一列读取到变量 @ln (这是 TranscodeService 追加的行号)
        sb.append("@ln) ");

        // --- 关键部分：赋值 ---
        // 设置 csvid 和 source_row_no
        sb.append("SET ");
        sb.append("csvid = ").append(splitId).append(", "); // 赋值当前切分ID
        sb.append("source_row_no = @ln"); // 将变量赋值给表字段

        return sb.toString();
    }

    private void finishDetail(QianyiDetail detail) {
        // 可以设置为 WAIT_VERIFY 等待后续校验，或者直接 PASS
        detail.setStatus(DetailStatus.WAIT_VERIFY); 
        detail.setProgress(100);
        detail.setErrorMsg(null);
        detailRepo.save(detail);
        log.info("源文件任务[{}] 装载完成", detail.getId());
    }
}