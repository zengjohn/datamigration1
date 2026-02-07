package com.example.moveprog.service;

import com.example.moveprog.dto.GlobalVerifyResult;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class GlobalVerifyService {

    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;
    private final JdbcHelper jdbcHelper;
    // 注入您现有的连接管理器，用于连目标库查 Count
    private final TargetDatabaseConnectionManager targetConnManager;

    /**
     * 执行全局一致性校验
     */
    public List<GlobalVerifyResult> performGlobalVerify(Long jobId) {
        List<GlobalVerifyResult> results = new ArrayList<>();

        // 1. 找出该 Job 涉及到的所有目标表名 (需要您在 Repository 里写好 findDistinctTargetTables)
        List<String> fullTableNames = detailRepo.findDistinctTargetTables(jobId);

        if (fullTableNames == null || fullTableNames.isEmpty()) {
            throw new RuntimeException("该作业下没有找到任何目标表记录");
        }

        for (String fullTableName : fullTableNames) {
            String[] parts = fullTableName.split("\\.");
            String schema = parts[0];
            String tableName = parts[1];

            GlobalVerifyResult res = new GlobalVerifyResult();
            res.setTableName(fullTableName);

            try {
                // --- A. 统计源端总行数 (IBM CSV) ---
                // 基于您之前的设计，这里利用 QianyiDetail 中的冗余字段进行 Sum
                Long sourceTotal = detailRepo.sumSourceRows(jobId, schema, tableName);
                res.setSourceRowCount(sourceTotal == null ? 0 : sourceTotal);

                // --- B. 统计拆分总行数 (UTF-8 Split) ---
                // 统计 csv_split 表的 row_count
                Long splitTotal = splitRepo.sumSplitRows(jobId, schema, tableName);
                res.setSplitRowCount(splitTotal == null ? 0 : splitTotal);

                // --- C. 统计目标库实际行数 (SELECT COUNT) ---
                Long targetTotal = countTargetTableRows(jobId, schema, tableName);
                res.setTargetRowCount(targetTotal);

                // --- D. 判定结论 ---
                // 必须三个数字都相等才算 MATCH
                boolean match = (res.getSourceRowCount().equals(res.getSplitRowCount())) &&
                                (res.getSplitRowCount().equals(res.getTargetRowCount()));
                
                if (match) {
                    res.setStatus("MATCH");
                } else {
                    // 简单的错误归因
                    if (!res.getSourceRowCount().equals(res.getSplitRowCount())) {
                        res.setStatus("MISMATCH_SPLIT"); // 转码/拆分阶段就不对
                    } else {
                        res.setStatus("MISMATCH_LOAD");  // 入库阶段不对
                    }
                }

            } catch (Exception e) {
                log.error("表[{}] 全局校验失败", fullTableName, e);
                res.setStatus("ERROR");
                res.setTargetRowCount(-1L);
            }

            results.add(res);
        }

        return results;
    }

    /**
     * 去目标库执行 SELECT COUNT(*)
     */
    private Long countTargetTableRows(Long jobId, String schema, String tableName) {
        // 使用 readOnly = true，如果配置了读写分离可以走从库，减少主库压力
        String fullTableName = jdbcHelper.tableNameQuote(schema, tableName);
        try (Connection conn = targetConnManager.getConnection(jobId, true);
             Statement stmt = conn.createStatement()) {
            
            String sql = "SELECT COUNT(*) FROM " + fullTableName;
            
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (Exception e) {
            log.warn("查询目标表行数失败: {}", fullTableName, e);
            throw new RuntimeException("目标库连接或查询失败", e);
        }
        return -1L;
    }
}