package com.example.moveprog.service;

import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.enums.JobStatus;
import com.example.moveprog.repository.MigrationJobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class JobService {

    private final MigrationJobRepository jobRepo;
    private final TargetDatabaseConnectionManager targetDatabaseConnectionManager;

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

}