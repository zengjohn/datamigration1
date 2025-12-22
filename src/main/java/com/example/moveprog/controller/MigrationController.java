package com.example.moveprog.controller;

import com.example.moveprog.entity.*;
import com.example.moveprog.enums.JobStatus;
import com.example.moveprog.repository.*;
import com.example.moveprog.service.JobControlManager;
import com.example.moveprog.service.StateManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/migration")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
@Slf4j
public class MigrationController {

    private final MigrationJobRepository jobRepo;
    private final QianyiRepository qianyiRepo;
    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;
    private final StateManager stateManager;

    // 假设你有一个内存管理器控制停止信号
    // 如果还没有，可以先暂时注释掉相关行，只改数据库状态
    private final JobControlManager controlManager;

    // ===========================
    // Level 1: 作业管理 (Job)
    // ===========================
    /**
     * 【新增】创建新作业
     */
    @PostMapping("/job/create")
    public ResponseEntity<?> createJob(@RequestBody MigrationJob job) {
        // 简单校验
        if (job.getName() == null || job.getSourceDirectory() == null) {
            return ResponseEntity.badRequest().body("名称和源目录不能为空");
        }

        // 默认状态
        job.setStatus(JobStatus.ACTIVE);

        // 保存
        jobRepo.save(job);

        // 这里通常会触发一次 Scanner，让后台立即去扫描该目录
        // scannerService.scanDirectory(job);

        return ResponseEntity.ok("作业创建成功");
    }

    @GetMapping("/jobs")
    public List<MigrationJob> listJobs() {
        // 按ID倒序，最新的在前面
        return jobRepo.findAll(org.springframework.data.domain.Sort.by("id").descending());
    }

    /**
     * 暂停或恢复作业
     * action: "stop" | "resume"
     */
    @PostMapping("/job/{id}/action")
    public ResponseEntity<?> jobAction(@PathVariable Long id, @RequestParam String action) {
        MigrationJob job = jobRepo.findById(id).orElseThrow(() -> new RuntimeException("Job not found"));

        if ("stop".equalsIgnoreCase(action)) {
            job.setStatus(JobStatus.STOPPED);
            jobRepo.save(job);
            if (controlManager != null) controlManager.stopJob(id); // 通知内存停止
            log.info("作业 [{}] 已停止", id);
        } else if ("resume".equalsIgnoreCase(action)) {
            job.setStatus(JobStatus.ACTIVE);
            jobRepo.save(job);
            if (controlManager != null) controlManager.resumeJob(id); // 清除内存停止标记
            log.info("作业 [{}] 已恢复", id);
        }
        return ResponseEntity.ok(Map.of("status", job.getStatus()));
    }

    // ===========================
    // Level 2: 文件明细 (Detail)
    // ===========================

    @GetMapping("/job/{jobId}/details")
    public List<QianyiDetail> listDetails(@PathVariable Long jobId) {
        return detailRepo.findByJobId(jobId);
    }

    // ===========================
    // Level 3: 切片详情 (Split)
    // ===========================

    @GetMapping("/detail/{detailId}/splits")
    public List<CsvSplit> listSplits(@PathVariable Long detailId) {
        return splitRepo.findByDetailId(detailId);
    }

    /**
     * 【核心运维功能】人工重试失败的分片
     * 前端只有在状态为 FAIL_LOAD 或 FAIL_VERIFY 时才会调用此接口
     */
    @PostMapping("/split/{id}/retry")
    public ResponseEntity<?> retrySplit(@PathVariable Long id) {
        try {
            // 调用 StateManager 将状态从 FAIL_X 重置为 WAIT_X
            stateManager.resetSplitForRetry(id);
            return ResponseEntity.ok("重试指令已下发，等待调度器执行");
        } catch (Exception e) {
            log.error("重试失败", e);
            return ResponseEntity.badRequest().body("重试失败: " + e.getMessage());
        }
    }

}