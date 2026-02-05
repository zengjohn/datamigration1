package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.enums.BatchStatus;
import com.example.moveprog.enums.JobStatus;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class JobService {

    private final MigrationJobRepository jobRepo;
    private final QianyiRepository qianyiRepository;
    private final TargetDatabaseConnectionManager targetDatabaseConnectionManager;
    private final MigrationArtifactManager migrationArtifactManager;

    private final AppProperties config;

    public List<MigrationJob> getAllJobs() {
        return jobRepo.findAll();
    }

    public MigrationJob getJob(Long id) {
        return jobRepo.findById(id).orElseThrow(() -> new RuntimeException("Job not found"));
    }

    @Transactional
    public MigrationJob createOrUpdateJob(MigrationJob job) {
        // 简单校验目录是否存在
        File dir = new File(job.getSourceDirectory());
        if (!dir.exists()) {
            dir.mkdirs(); // 或者抛错，看需求
        }
        return jobRepo.save(job);
    }

    @Transactional
    public void stopJob(Long jobId) {
        // 2. 【新增】清理连接池资源
        targetDatabaseConnectionManager.invalidateJob(jobId);
    }

    @Transactional
    public void deleteJob(Long id) {
        jobRepo.deleteById(id);
    }

    /**
     * 结单逻辑 (Close Batch)
     */
    @Transactional
    public void closeBatch(Long batchId) {
        Qianyi batch = qianyiRepository.findById(batchId).orElseThrow();

        // 1. 检查是否可以结单 (比如是否还有运行中的任务)
        // checkStatus(batch);

        // 2. 更新数据库状态
        batch.setStatus(BatchStatus.FINISHED); // 或 CLOSED
        qianyiRepository.save(batch);

        // 3. 【核心】触发清理 (根据配置)
        // 建议从配置读取: app.job.auto-clean-on-close = true
        if (config.getJob().isAutoCleanOnClose()) {
            // 异步执行清理，防止阻塞前端响应 (文件多时可能删很久)
            CompletableFuture.runAsync(() -> {
                migrationArtifactManager.cleanBatchArtifacts(batchId);
            });
        }
    }

}