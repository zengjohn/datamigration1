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
import org.springframework.transaction.annotation.Transactional;

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

    // 机器 A 只抢占 机器 A 的任务
    // --- 2. 调度逻辑 (每 5 秒轮询) ---
    @Scheduled(fixedDelay = 5000)
    public void schedule() {
        String myIp = config.getCurrentNodeIp(); // 从配置获取本机 IP
        // 防御性检查：如果没配，抛异常或者打印严重警告
        if (myIp == null || myIp.isEmpty()) {
            log.error("严重错误：未配置 app.current-node-ip，调度器无法工作！");
            return;
        }

        dispatchTranscode(myIp);
        dispatchLoad(myIp);
        dispatchVerify(myIp);
    }

    /**
     * 机器 A 只负责救援 机器 A 的僵尸任务
     * 每 10 分钟运行一次
     * 检查那些"卡住"超过 30 分钟的任务
     */
    @Scheduled(fixedDelay = 600000)
    public void rescueStuckTasks() {
        String myIp = config.getCurrentNodeIp();

        LocalDateTime timeThreshold = LocalDateTime.now().minusMinutes(30);

        // 1. 捞出处于 LOADING 状态 且 最后更新时间早于 30分钟前 的任务
        List<CsvSplit> stuckLoadingSplits = splitRepo.findByStatusAndNodeIdAndUpdateTimeBefore(CsvSplitStatus.LOADING, myIp, timeThreshold);
        for (CsvSplit split : stuckLoadingSplits) {
            log.error("本机[{}]发现僵尸任务 Split[{}]，卡在 LOADING 超过30分钟，强制重置。", myIp, split.getId());
            // 强制重置状态
            stateManager.switchSplitStatus(split.getId(), CsvSplitStatus.WAIT_LOAD, "系统自动重置Loading超时任务");
        }

        // 2. 捞出处于 VERIFYING 状态 且 最后更新时间早于 30分钟前 的任务
        List<CsvSplit> stuckVerifyingSplits = splitRepo.findByStatusAndNodeIdAndUpdateTimeBefore(CsvSplitStatus.LOADING, myIp, timeThreshold);
        for (CsvSplit split : stuckVerifyingSplits) {
            log.error("本机[{}]发现僵尸任务 Split[{}]，卡在 VERIFYING 超过30分钟，强制重置。", myIp, split.getId());
            // 强制重置状态
            stateManager.switchSplitStatus(split.getId(), CsvSplitStatus.WAIT_VERIFY, "系统自动重置Verifying超时任务");
        }

    }

    /**
     * --- 阶段 1: 调度转码 (针对 Detail) ---
     * @param myIp
     */
    @Transactional // 必须开启事务
    public void dispatchTranscode(String myIp) {
        // 1. 使用 SKIP LOCKED 抢占 5 个任务
        List<QianyiDetail> details = detailRepo.findAndLockTop5ByStatusAndNodeId(DetailStatus.NEW.toString(), myIp);
        if (details.isEmpty()) return;

        for (QianyiDetail d : details) {
            // 2. 立即在事务内标记为“处理中”，防止事务提交后锁释放被别人抢走
            // (虽然用了 SKIP LOCKED 别人本身就查不到，但为了逻辑严谨，先改状态)
            int rows = detailRepo.updateStatus(d.getId(), DetailStatus.NEW, DetailStatus.TRANSCODING);

            if (rows > 0) {
                // 3. 异步提交给线程池处理实际业务
                // 注意：这里不要直接在这里跑耗时逻辑，否则数据库连接会一直被事务占用
                // 应该把 task.getId() 丢给线程池
                transcodeExecutor.execute(() -> {
                    transcodeService.execute(d.getId());
                });
            }
        }
    }

    /**
     * --- 阶段 2: 调度装载 (针对 Split) ---
     *  【变化点】这里不再查 Detail，而是直接查 Split 表
     * @param myIp
     */
    @Transactional // 必须开启事务
    public void dispatchLoad(String myIp) {
        // 注意：这里可以一次取更多，因为装载通常比转码快
        List<CsvSplit> splits = splitRepo.findAndLockTop20ByStatusAndNodeId(CsvSplitStatus.WAIT_LOAD.toString(), myIp);
        for (CsvSplit s : splits) {
            if (inFlightSplits.contains(s.getId())) continue; // 防抖

            // 2. 【核心修改】直接调用 Repo 更新，不走 StateManager
            // 这里的逻辑极快，纯 DB 操作，完全在一个事务内
            int rows = splitRepo.updateStatus(s.getId(), CsvSplitStatus.WAIT_LOAD, CsvSplitStatus.LOADING);
            if (rows > 0) {
                // 更新成功，内存加锁
                inFlightSplits.add(s.getId());
                // 异步提交
                loadExecutor.execute(() -> {
                    try {
                        loadService.execute(s.getId());
                    } finally {
                        inFlightSplits.remove(s.getId());
                    }
                });
            }

        }
    }

    // --- 阶段 3: 调度校验 (针对 Split) ---
    @Transactional // 必须开启事务
    public void dispatchVerify(String myIp) {
        // SELECT * FROM t_split WHERE status = 'WAIT_VERIFY' LIMIT 20
        List<CsvSplit> splits = splitRepo.findAndLockTop20ByStatusAndNodeId(CsvSplitStatus.WAIT_VERIFY.toString(), myIp);
        for (CsvSplit s : splits) {
            if (inFlightSplits.contains(s.getId())) continue;

            int rows = splitRepo.updateStatus(s.getId(), CsvSplitStatus.WAIT_VERIFY, CsvSplitStatus.VERIFYING);
            if (rows > 0) {
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
}