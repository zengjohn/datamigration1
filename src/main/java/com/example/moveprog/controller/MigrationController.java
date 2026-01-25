package com.example.moveprog.controller;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.*;
import com.example.moveprog.enums.BatchStatus;
import com.example.moveprog.enums.CsvSplitStatus;
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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;

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

    @PostMapping("/batch/{id}/close")
    public ResponseEntity<?> closeBatch(@PathVariable Long id) {
        Qianyi batch = qianyiRepo.findById(id).orElseThrow();

        // 1. 检查是否所有 Detail 都已结束
        long unfinishedCount = detailRepo.countByQianyiIdAndStatusNot(id, DetailStatus.FINISHED);
        long errorCount = detailRepo.countByQianyiIdAndStatusNot(id, DetailStatus.FINISHED_WITH_ERROR); // 假设你有这个状态

        if (unfinishedCount > 0) {
            return ResponseEntity.badRequest().body("还有 " + unfinishedCount + " 个文件未处理完，无法结单");
        }

        // 2. 修改状态
        batch.setStatus(BatchStatus.FINISHED); // 或 CLOSED
        batch.setUpdateTime(LocalDateTime.now());
        qianyiRepo.save(batch);

        return ResponseEntity.ok("批次已结单" + (errorCount > 0 ? " (注意：包含失败任务)" : ""));
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

        // 1. 删切片
        splitRepo.deleteByQianyiId(id);
        // 2. 删明细
        detailRepo.deleteByQianyiId(id);
        // 3. 删批次主记录
        qianyiRepo.deleteById(id);

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

    @GetMapping("/batch/{batchId}/details")
    public Page<QianyiDetail> details(
            @PathVariable Long batchId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {

        // 按 ID 倒序，方便看最新的
        return detailRepo.findByQianyiId(batchId,
                PageRequest.of(page, size, Sort.by("id").descending()));
    }

    @PostMapping("/detail/{id}/retry-transcode")
    public ResponseEntity<?> retryTranscode(@PathVariable Long id) {
        // 逻辑：将状态重置为 NEW，触发 TranscodeService 重新运行
        stateManager.updateDetailStatus(id, DetailStatus.NEW);
        return ResponseEntity.ok("转码重试指令已下发");
    }

    /**
     * 【新增】读取转码失败的脏数据文件 (.bad)
     */
    @GetMapping("/detail/{id}/bad-content")
    public ResponseEntity<?> getBadContent(@PathVariable Long id) {
        try {
            // 1. 推算 .bad 文件路径
            // 假设规则是：在配置的 error 目录下，文件名与源文件相同，后缀加 .bad
            // 或者您在 TranscodeService 里生成的路径规则
            Path badFileName = Paths.get(config.getTranscodeJob().getErrorDir(), id + "_error.csv");
            if (!badFileName.toFile().exists()) {
                return ResponseEntity.ok("暂无失败记录文件 (或文件已被清理)");
            }

            // 2. 读取前 100 行
            List<String> lines = Files.readAllLines(badFileName);
            StringBuilder sb = new StringBuilder();
            sb.append("--- 失败行预览 (Top 100) ---\n");
            sb.append("文件路径: ").append(badFileName).append("\n\n");

            int limit = Math.min(lines.size(), 100);
            for (int i = 0; i < limit; i++) {
                sb.append(lines.get(i)).append("\n");
            }
            if (lines.size() > limit) {
                sb.append("\n... (剩余 ").append(lines.size() - limit).append(" 行未显示) ...");
            }

            return ResponseEntity.ok(sb.toString());

        } catch (Exception e) {
            return ResponseEntity.badRequest().body("读取失败: " + e.getMessage());
        }
    }

    // ===========================
    // Level 3: 切片详情 (Split)
    // ===========================

    @GetMapping("/detail/{detailId}/splits")
    public List<CsvSplit> listSplits(@PathVariable Long detailId) {
        return splitRepo.findByDetailId(detailId);
    }

    /**
     * 【重试作业】(Nuclear Option)
     * 语义：清理该切片在数据库的数据，重新执行 Load -> Verify 流程
     * 目标状态：WAIT_LOAD
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
     * 【重新验证】(Light Option)
     * 语义：保留数据库数据，仅重新执行 Verify 流程
     * 目标状态：WAIT_VERIFY
     * 适用场景：
     * 1. 校验不通过，人工修复了数据库数据后，想验证是否修好了
     * 2. 校验通过，但怀疑校验逻辑有漏洞，想改了代码后再跑一次
     */
    @PostMapping("/split/{id}/reverify")
    public ResponseEntity<?> forceReVerify(@PathVariable Long id) {
        CsvSplit split = splitRepo.findById(id)
                .orElseThrow(() -> new RuntimeException("切片(id=" + id + ")不存在"));

        // 1. 安全检查：只有以下状态允许重验
        // PASS (已通过), FAIL_VERIFY (校验失败), FINISHED (已完成)
        // 绝对不能重置正在 LOADING 或 VERIFYING 的任务
        if (split.getStatus() == CsvSplitStatus.LOADING ||
                split.getStatus() == CsvSplitStatus.VERIFYING) {
            return ResponseEntity.badRequest().body("当前状态正在运行中，无法强制重验");
        }

        Long qianyiId = split.getQianyiId();
        Qianyi qianyiById = qianyiRepo.findById(qianyiId)
                .orElseThrow(() -> new RuntimeException("批次(id=" + qianyiId + ")不存在"));
        if (BatchStatus.FINISHED.equals(qianyiById.getStatus())) {
            return ResponseEntity.badRequest().body("当前批次状态已经关单，无法强制重验");
        }

        // 2. 执行重置
        // 将状态改回 WAIT_VERIFY，清空错误信息，调度器会自动捡起它
        split.setStatus(CsvSplitStatus.WAIT_VERIFY);
        split.setErrorMsg(null); // 清空旧的报错/差异信息
        splitRepo.save(split);

        // 3. 物理清理 (必须要做)
        // 避免“新验证通过了，但旧的错误文件还在”的尴尬情况
        deleteDiffFile(split.getId());

        return ResponseEntity.ok("已加入重验队列");
    }

    private void deleteDiffFile(Long splitId) {
        try {
            String basePath = config.getVerify().getVerifyResultBasePath();
            Path path = Paths.get(basePath, "split_" + splitId + "_diff.txt");
            Files.deleteIfExists(path);
            log.info("已清理旧差异文件: {}", path);
        } catch (Exception e) {
            log.warn("清理旧差异文件失败 (不影响主流程): {}", e.getMessage());
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