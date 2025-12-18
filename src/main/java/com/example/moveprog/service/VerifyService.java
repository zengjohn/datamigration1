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

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class VerifyService {

    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;
    private final QianyiRepository qianyiRepo;
    private final MigrationJobRepository jobRepo;
    private final TaskLockManager lockManager;
    private final JdbcTemplate jdbcTemplate;

    @Async
    public void execute(Long detailId) {
        QianyiDetail detail = detailRepo.findById(detailId).orElse(null);
        if (detail == null) {
            lockManager.releaseLock(detailId);
            return;
        }

        try {
            log.info("开始校验任务: {}", detailId);
            
            // 1. 获取目标表名
            Qianyi batch = qianyiRepo.findById(detail.getQianyiId()).orElseThrow();
            String tableName = batch.getTableName();

            // 2. 获取该任务下所有成功的切分文件
            List<CsvSplit> splits = splitRepo.findByDetailIdAndStatusNot(detailId, CsvSplitStatus.FAIL);
            // 注意：这里应该只校验 PASS 的，如果还有 WL 的话说明流程没走完，逻辑上不应该进这里

            long totalSourceRows = 0;
            long totalTargetRows = 0;
            boolean allMatch = true;

            for (CsvSplit split : splits) {
                if (split.getStatus() != CsvSplitStatus.PASS) {
                    allMatch = false;
                    log.warn("发现未完成的切分文件: ID={}", split.getId());
                    break;
                }

                // A. 源端行数
                Long sourceRows = split.getRowCount();

                // B. 目标端行数 (查询数据库)
                String sql = String.format("SELECT COUNT(*) FROM %s WHERE csvid = ?", tableName);
                Long targetRows = jdbcTemplate.queryForObject(sql, Long.class, split.getId());

                if (targetRows == null || !targetRows.equals(sourceRows)) {
                    allMatch = false;
                    String error = String.format("行数不匹配! SplitId=%d, Source=%d, Target=%d", 
                                                 split.getId(), sourceRows, targetRows);
                    log.error(error);
                    detail.setErrorMsg(error);
                    break;
                }

                totalSourceRows += sourceRows;
                totalTargetRows += targetRows;
            }

            // 3. 更新状态
            if (allMatch) {
                detail.setStatus(DetailStatus.PASS);
                detail.setErrorMsg(null); // 清空错误
                log.info("校验通过! TaskId={}, 总行数={}", detailId, totalTargetRows);
            } else {
                detail.setStatus(DetailStatus.WAIT_VERIFY); // 保持在待校验状态，或者 FAIL_LOAD
                // 这里策略可以是报警，或者人工介入
                detail.setErrorMsg(detail.getErrorMsg() + " [校验失败]");
            }
            
            detailRepo.save(detail);

        } catch (Exception e) {
            log.error("校验过程异常", e);
            detail.setErrorMsg("校验异常: " + e.getMessage());
            // 状态保持 WAIT_VERIFY 或转为人工处理
            detailRepo.save(detail);
        } finally {
            lockManager.releaseLock(detailId);
        }
    }
}