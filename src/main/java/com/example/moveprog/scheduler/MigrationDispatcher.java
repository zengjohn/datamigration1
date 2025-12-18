package com.example.moveprog.scheduler;

import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.BatchStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.example.moveprog.repository.QianyiRepository;
import com.example.moveprog.service.LoadService;
import com.example.moveprog.service.TranscodeService;
import com.example.moveprog.service.VerifyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class MigrationDispatcher {

    private final QianyiDetailRepository detailRepo;
    private final QianyiRepository qianyiRepo;
    private final TranscodeService transcodeService;
    private final LoadService loadService;
    private final VerifyService verifyService;

    private final TaskLockManager lockManager;

    @Scheduled(fixedDelay = 2000)
    public void dispatch() {
        // 1. 扫描 NEW (待转码)
        List<QianyiDetail> newTasks = detailRepo.findByStatus(DetailStatus.NEW);
        for (QianyiDetail task : newTasks) {
            if (lockManager.tryLock(task.getId())) {
                transcodeService.execute(task.getId()); // 异步执行
            }
        }

        // 2. 扫描 WAIT_LOAD (待装载) 或 WAIT_RELOAD (断点续传)
        List<QianyiDetail> loadTasks = detailRepo.findByStatusIn(List.of(DetailStatus.WAIT_LOAD, DetailStatus.WAIT_RELOAD));
        for (QianyiDetail task : loadTasks) {
            if (lockManager.tryLock(task.getId())) {
                loadService.execute(task.getId()); // 异步执行
            }
        }

        // 3. 新增：验证调度
        List<QianyiDetail> verifyTasks = detailRepo.findByStatus(DetailStatus.WAIT_VERIFY);
        for (QianyiDetail task : verifyTasks) {
            if (lockManager.tryLock(task.getId())) {
                log.info("调度验证任务: {}", task.getId());
                verifyService.execute(task.getId()); // 异步执行
            }
        }

        // 4. 批次完成检查 ...
        checkBatchCompletion();
    }

    private void checkBatchCompletion() {
        // 查找所有 PROCESSING 的主单
        List<Qianyi> runningBatches = qianyiRepo.findByStatus(BatchStatus.PROCESSING);
        for (Qianyi batch : runningBatches) {
            // 检查旗下是否还有未 PASS 的 Detail
            long notFinishedCount = detailRepo.countByQianyiIdAndStatusNot(batch.getId(), DetailStatus.PASS);
            if (notFinishedCount == 0) {
                batch.setStatus(BatchStatus.FINISHED);
                qianyiRepo.save(batch);
                log.info(">>> 批次完成: JobId={}, Table={}", batch.getId(), batch.getTableName());
            }
        }
    }
}