package com.example.moveprog.service;

import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.enums.JobStatus;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Slf4j
public class StateManager {

    @Autowired private CsvSplitRepository splitRepo;
    @Autowired private QianyiDetailRepository detailRepo;
    @Autowired private MigrationJobRepository jobRepo;

    /**
     * 【核心】尝试切换 Split 状态 (双事务的核心：独立事务提交)
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean switchSplitStatus(Long splitId, CsvSplitStatus target, String msg) {
        CsvSplit split = splitRepo.findById(splitId).orElseThrow();

        // 1. 检查全局作业是否停止 ((全局刹车)
        MigrationJob job = jobRepo.findById(split.getJobId()).orElseThrow();
        if (job.getStatus() == JobStatus.STOPPED || job.getStatus() == JobStatus.PAUSED) {
            log.warn("作业已停止，拒绝 Split[{}] 流转到 {}", splitId, target);
            return false;
        }

        // 2. 状态机检查 (乐观锁逻辑，防止并发)
        if (!split.getStatus().canTransitionTo(target)) {
            log.info("状态流转非法: {} -> {}", split.getStatus(), target);
            return false; // 抢占失败
        }

        // 3. 执行更新
        split.setStatus(target);
        if (msg != null) split.setErrorMsg(msg);
        splitRepo.save(split);
        return true;
    }

    /**
     * 父级状态流转 (Transcode用)
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateDetailStatus(Long detailId, DetailStatus status) {
        QianyiDetail detail = detailRepo.findById(detailId).orElseThrow();
        detail.setStatus(status);
        detailRepo.save(detail);
    }

    /**
     * 【重试逻辑】重置状态
     * 供 Controller 手动调用，用于将 FAIL 状态拉回到 WAIT 状态
     */
    @Transactional
    public void resetSplitForRetry(Long splitId) {
        CsvSplit split = splitRepo.findById(splitId).orElseThrow();
        
        // 只有失败状态才允许重试
        if (split.getStatus() == CsvSplitStatus.FAIL_LOAD) {
            split.setStatus(CsvSplitStatus.WAIT_LOAD);
            split.setErrorMsg("人工重试-等待装载");
        } else if (split.getStatus() == CsvSplitStatus.FAIL_VERIFY) {
            split.setStatus(CsvSplitStatus.WAIT_VERIFY);
            split.setErrorMsg("人工重试-等待验证");
        } else {
            throw new RuntimeException("当前状态不允许重试");
        }
        splitRepo.save(split);
    }

    /**
     * 刷新父级 Detail 的状态
     * 逻辑：统计归属该 Detail 的所有 Split 的状态分布
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void refreshDetailStatus(Long detailId) {
        QianyiDetail detail = detailRepo.findById(detailId).orElseThrow();
        List<CsvSplit> splits = splitRepo.findByDetailId(detailId);

        if (splits.isEmpty()) {
            return; // 还没有分片，保持原样
        }

        boolean hasFail = false;
        boolean hasRunning = false;
        boolean allFinished = true;

        for (CsvSplit s : splits) {
            switch (s.getStatus()) {
                case LOADING:
                case VERIFYING:
                case WAIT_LOAD:
                case WAIT_VERIFY:
                    hasRunning = true;
                    allFinished = false;
                    break;
                case FAIL_LOAD:
                case FAIL_VERIFY:
                    hasFail = true;
                    // 注意：即使有失败，如果不重试，也算该分片这就结束了，所以不把 allFinished 置为 false
                    // 但为了严谨，通常失败也意味着流程没走完，看你业务定义。
                    // 这里假设：失败状态也是一种"终态"（直到人工重试）
                    break;
                case PASS:
                    // 成功，继续
                    break;
            }
        }

        DetailStatus newStatus;
        if (hasRunning) {
            newStatus = DetailStatus.PROCESSING_CHILDS;
        } else if (hasFail) {
            newStatus = DetailStatus.FINISHED_WITH_ERROR; // 跑完了，但有错
        } else {
            newStatus = DetailStatus.FINISHED; // 全部 PASS
        }

        // 只有状态变了才更新，减少数据库写操作
        if (detail.getStatus() != newStatus) {
            detail.setStatus(newStatus);
            detailRepo.save(detail);
            log.info("父文件 Detail[{}] 状态同步为: {}", detailId, newStatus);
        }
    }

}