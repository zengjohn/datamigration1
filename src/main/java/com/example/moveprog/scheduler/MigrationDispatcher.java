package com.example.moveprog.scheduler;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.example.moveprog.service.LoadService;
import com.example.moveprog.service.StateManager;
import com.example.moveprog.service.TranscodeService;
import com.example.moveprog.service.VerifyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

@Configuration
@EnableScheduling
@Slf4j
@RequiredArgsConstructor
public class MigrationDispatcher {

    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;
    
    private final TranscodeService transcodeService;
    private final LoadService loadService;
    private final VerifyService verifyService;
    private final StateManager stateManager;

    // 注入 AppProperties 用于获取配置...
    private final AppProperties config;

    // 内存防抖 Set (防止重复提交到队列)
    private Set<Long> inFlightSplits = ConcurrentHashMap.newKeySet();

    // --- 1. 线程池配置 (隔离) ---
    @Bean("transcodeExecutor")
    public Executor transcodeExecutor() {
        AppProperties.ThreadPool transcode = config.getMigrationThreadPool().getTranscode();
        ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
        ex.setCorePoolSize(transcode.getCorePoolSize() > 0 ? transcode.getCorePoolSize() : 2);
        ex.setMaxPoolSize(transcode.getMaxPoolSize() > 0 ? transcode.getMaxPoolSize() : 4);
        ex.setThreadNamePrefix("Task-Trans-");
        ex.initialize();
        return ex;
    }

    @Bean("loadExecutor")
    public Executor loadExecutor() {
        AppProperties.ThreadPool load = config.getMigrationThreadPool().getLoad();
        ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
        ex.setCorePoolSize(load.getCorePoolSize() > 0 ? load.getCorePoolSize() : 5);
        ex.setMaxPoolSize(load.getMaxPoolSize() > 0 ? load.getMaxPoolSize() : 10);
        ex.setThreadNamePrefix("Task-Load-");
        ex.initialize();
        return ex;
    }

    @Bean("verifyExecutor")
    public Executor verifyExecutor() {
        AppProperties.ThreadPool verify = config.getMigrationThreadPool().getVerify();
        ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
        ex.setCorePoolSize(verify.getCorePoolSize() > 0 ? verify.getCorePoolSize() : 5);
        ex.setMaxPoolSize(verify.getMaxPoolSize() > 0  ? verify.getMaxPoolSize() : 10);
        ex.setThreadNamePrefix("Task-Verify-");
        ex.initialize();
        return ex;
    }

    @Autowired private Executor transcodeExecutor;
    @Autowired private Executor loadExecutor;
    @Autowired private Executor verifyExecutor;

    // --- 2. 调度逻辑 (每 5 秒轮询) ---
    @Scheduled(fixedDelay = 5000)
    public void schedule() {
        dispatchTranscode();
        dispatchLoad();
        dispatchVerify();
    }

    /**
     * 每 10 分钟运行一次
     * 检查那些"卡住"超过 30 分钟的任务
     */
    @Scheduled(fixedDelay = 600000)
    public void rescueStuckTasks() {
        LocalDateTime timeThreshold = LocalDateTime.now().minusMinutes(30);

        // 1. 捞出处于 LOADING 状态 且 最后更新时间早于 30分钟前 的任务
        List<CsvSplit> stuckLoadingSplits = splitRepo.findByStatusAndUpdateTimeBefore(CsvSplitStatus.LOADING, timeThreshold);
        for (CsvSplit split : stuckLoadingSplits) {
            log.error("发现僵尸任务 Split[{}]，卡在 LOADING 超过30分钟，强制重置。", split.getId());
            // 强制重置状态
            stateManager.switchSplitStatus(split.getId(), CsvSplitStatus.WAIT_LOAD, "系统自动重置Loading超时任务");
        }

        // 2. 捞出处于 VERIFYING 状态 且 最后更新时间早于 30分钟前 的任务
        List<CsvSplit> stuckVerifyingSplits = splitRepo.findByStatusAndUpdateTimeBefore(CsvSplitStatus.LOADING, timeThreshold);
        for (CsvSplit split : stuckVerifyingSplits) {
            log.error("发现僵尸任务 Split[{}]，卡在 VERIFYING 超过30分钟，强制重置。", split.getId());
            // 强制重置状态
            stateManager.switchSplitStatus(split.getId(), CsvSplitStatus.WAIT_VERIFY, "系统自动重置Verifying超时任务");
        }

    }

    // --- 阶段 1: 调度转码 (针对 Detail) ---
    private void dispatchTranscode() {
        // 1. 查出所有待转码的文件
        // SELECT * FROM t_detail WHERE status = 'NEW' LIMIT 5
        List<QianyiDetail> details = detailRepo.findTop5ByStatus(DetailStatus.NEW);
        for (QianyiDetail d : details) {
            // 这里简单起见没加 Set 防抖，因为 Transcode 比较少，如有需要也可加
            transcodeExecutor.execute(() -> transcodeService.execute(d.getId()));
        }
    }

    // --- 阶段 2: 调度装载 (针对 Split) ---
    // 【变化点】这里不再查 Detail，而是直接查 Split 表
    private void dispatchLoad() {
        // SELECT * FROM t_split WHERE status = 'WAIT_LOAD' LIMIT 20
        // 注意：这里可以一次取更多，因为装载通常比转码快
        List<CsvSplit> splits = splitRepo.findTop20ByStatus(CsvSplitStatus.WAIT_LOAD);
        for (CsvSplit s : splits) {
            if (inFlightSplits.contains(s.getId())) continue; // 防抖
            inFlightSplits.add(s.getId());

            loadExecutor.execute(() -> {
                try {
                    loadService.execute(s.getId());
                } finally {
                    inFlightSplits.remove(s.getId());
                }
            });
        }
    }

    // --- 阶段 3: 调度校验 (针对 Split) ---
    private void dispatchVerify() {
        // SELECT * FROM t_split WHERE status = 'WAIT_VERIFY' LIMIT 20
        List<CsvSplit> splits = splitRepo.findTop20ByStatus(CsvSplitStatus.WAIT_VERIFY);
        for (CsvSplit s : splits) {
            if (inFlightSplits.contains(s.getId())) continue;
            inFlightSplits.add(s.getId());

            verifyExecutor.execute(() -> {
                try {
                    verifyService.execute(s.getId());
                } finally {
                    inFlightSplits.remove(s.getId());
                }
            });
        }
    }
}