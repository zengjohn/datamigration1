package com.example.moveprog.controller;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.*;
import com.example.moveprog.enums.BatchStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.enums.JobStatus;
import com.example.moveprog.repository.*;
import com.example.moveprog.service.JobControlManager;
import com.example.moveprog.service.StateManager;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    private final AppProperties config; // 注入配置以获取 Verify 结果目录

    // ===========================
    // Level 1: 作业管理 (Job)
    // ===========================
    /**
     * 【新增】创建新作业
     */
    @PostMapping("/job/create")
    public ResponseEntity<?> createJob(@RequestBody MigrationJob job) {
        // 简单校验
        if (job.getName() == null ||
                job.getSourceDirectory() == null ||
                job.getTargetDbUrl() == null ||
                job.getTargetDbUser() == null) {
            return ResponseEntity.badRequest().body("名称, 源目录, 目标库url, 目标库用户名 不能为空");
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

    /**
     * 【重试批次】
     * 逻辑：删除解析失败的记录，让扫描器在几秒后重新抓取
     */
    @PostMapping("/batch/{id}/retry")
    public ResponseEntity<?> retryBatch(@PathVariable Long id) {
        Qianyi qianyi = qianyiRepo.findById(id).orElseThrow(() -> new RuntimeException("记录不存在"));

        if (qianyi.getStatus() != BatchStatus.FAIL_PARSE) {
            return ResponseEntity.badRequest().body("只有解析失败(FAIL_PARSE)的任务才能重试");
        }

        // 级联删除明细（如果有残留的）
        List<QianyiDetail> details = detailRepo.findByQianyiId(id);
        if (Objects.nonNull(details) && details.size() > 0) {
            for (QianyiDetail detail : details) {
                detailRepo.delete(detail);
            }
        }

        // 删除主记录
        qianyiRepo.delete(qianyi);

        return ResponseEntity.ok("重试指令已下发（记录已清除，等待下一次扫描）");
    }

    // Level 1.5: 批次管理 (OK文件) - 【新增】
    // ===========================
    @GetMapping("/job/{jobId}/batches")
    public List<Qianyi> listBatches(@PathVariable Long jobId) {
        // 展示该作业下的 OK 文件处理情况
        return qianyiRepo.findByJobId(jobId); // 需要在 QianyiRepo 加这个方法
    }

    // ===========================
    // Level 2: 文件明细 (Detail)
    // ===========================

    @GetMapping("/job/{jobId}/details")
    public Page<QianyiDetail> listDetails(
            @PathVariable Long jobId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {

        // 按 ID 倒序，方便看最新的
        return detailRepo.findByJobId(jobId,
                PageRequest.of(page, size, Sort.by("id").descending()));
    }

    @PostMapping("/detail/{id}/retry-transcode")
    public ResponseEntity<?> retryTranscode(@PathVariable Long id) {
        // 逻辑：将状态重置为 NEW，触发 TranscodeService 重新运行
        stateManager.updateDetailStatus(id, DetailStatus.NEW);
        return ResponseEntity.ok("转码重试指令已下发");
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

    /**
     * 【新增】读取差异文件内容用于前端展示
     */
    @GetMapping("/split/{id}/diff-content")
    public ResponseEntity<?> getDiffContent(@PathVariable Long id) {
        try {
            // 构造差异文件路径 (规则需与 VerifyService 一致)
            String filename = "split_" + id + "_diff.txt";
            Path path = Paths.get(config.getVerify().getVerifyResultBasePath(), filename);

            if (!Files.exists(path)) {
                return ResponseEntity.ok("暂无差异文件 (可能校验通过或文件已被清理)");
            }

            // 读取前 100 行 (防止文件过大撑爆浏览器)
            List<String> lines = Files.readAllLines(path);
            StringBuilder sb = new StringBuilder();
            int limit = Math.min(lines.size(), 200);
            for(int i=0; i<limit; i++) {
                sb.append(lines.get(i)).append("\n");
            }
            if (lines.size() > limit) {
                sb.append("\n... (更多内容请在服务器查看) ...");
            }

            return ResponseEntity.ok(sb.toString());
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("读取失败: " + e.getMessage());
        }
    }

}