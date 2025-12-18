package com.example.moveprog.controller;

import com.example.moveprog.entity.*;
import com.example.moveprog.enums.BatchStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.repository.*;
import com.example.moveprog.scheduler.TaskLockManager;
import com.example.moveprog.service.LoadService;
import com.example.moveprog.service.TranscodeService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class MigrationController {

    private final MigrationJobRepository jobRepo;
    private final QianyiRepository qianyiRepo;
    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;
    
    // 简单的重试逻辑需要调用Service，或者重置状态让调度器去捡
    private final TaskLockManager lockManager;

    // ================= 1. Job Level (作业配置) =================
    
    @GetMapping("/job/list")
    public List<MigrationJob> listJobs() {
        return jobRepo.findAll();
    }

    @PostMapping("/job/save")
    public MigrationJob saveJob(@RequestBody MigrationJob job) {
        return jobRepo.save(job);
    }

    @PostMapping("/job/toggle/{id}")
    public void toggleJob(@PathVariable Long id) {
        MigrationJob job = jobRepo.findById(id).orElseThrow();
        job.setActive(!job.isActive());
        jobRepo.save(job);
    }

    // ================= 2. Qianyi Level (批次/实例) =================

    @GetMapping("/batch/list")
    public List<Qianyi> listBatches(@RequestParam Long jobId) {
        // 按时间倒序
        return qianyiRepo.findByStatus(BatchStatus.PROCESSING); // 这里根据你的Repository自定义查询，或者用 Example
        // 简单演示：
        // return qianyiRepo.findAll(Sort.by(Sort.Direction.DESC, "id")); 
        // 实际建议在Repo里加: List<Qianyi> findByJobIdOrderByIdDesc(Long jobId);
    }

    // ================= 3. Detail Level (源文件任务) =================

    @GetMapping("/detail/list")
    public List<QianyiDetail> listDetails(@RequestParam Long batchId) {
        // 实际建议在Repo里加: List<QianyiDetail> findByQianyiIdOrderById(Long qianyiId);
        // 这里为了编译通过，演示用 findAll 过滤 (生产环境请改写 Repository)
        return detailRepo.findAll().stream()
                .filter(d -> d.getQianyiId().equals(batchId))
                .toList();
    }

    // 重试某个源文件任务 (状态重置为 NEW 或 WAIT_LOAD)
    @PostMapping("/detail/retry/{id}")
    public void retryDetail(@PathVariable Long id, @RequestParam String type) {
        QianyiDetail detail = detailRepo.findById(id).orElseThrow();
        if ("TRANSCODE".equals(type)) {
            detail.setStatus(DetailStatus.NEW);
            detail.setProgress(0);
            detail.setErrorMsg(null);
            // 还需要清理旧的 split 记录: splitRepo.deleteByDetailId(id);
        } else if ("LOAD".equals(type)) {
            detail.setStatus(DetailStatus.WAIT_LOAD);
            detail.setErrorMsg(null);
        }
        detailRepo.save(detail);
    }

    // ================= 4. Split Level (切分/装载单元) =================
    
    @GetMapping("/split/list")
    public List<CsvSplit> listSplits(@RequestParam Long detailId) {
        return splitRepo.findByDetailId(detailId);
    }
}