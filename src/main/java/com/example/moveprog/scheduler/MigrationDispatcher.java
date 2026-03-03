package com.example.moveprog.scheduler;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.enums.JobStatus;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.MigrationJobRepository;
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
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

@Configuration
@EnableScheduling
@Slf4j
@RequiredArgsConstructor
public class MigrationDispatcher {

    private final MigrationJobRepository jobRepo;
    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;
    
    private final TranscodeService transcodeService;
    private final LoadService loadService;
    private final VerifyService verifyService;
    private final StateManager stateManager;

    // 注入 AppProperties 用于获取配置...
    private final AppProperties appProperties;

    /**
     * Self-injection to solve AOP self-invocation issue.
     * Uses @Lazy to avoid circular dependency during bean initialization.
     */
    @Autowired
    @Lazy
    private MigrationDispatcher self;

    // 内存防抖 Set (防止重复提交到队列)
    private Set<Long> inFlightSplits = ConcurrentHashMap.newKeySet();

    // --- 信号量限流 (保护数据库连接池, 不再 final，改为在构造后初始化) ---
    private Semaphore loadSemaphore;
    // 校验任务通常比较慢，并发太高容易把数据库读IO打满，设置保守一些
    private Semaphore verifySemaphore;

    // Keep track of current limits for UI display
    private int currentLoadLimit;
    private int currentVerifyLimit;

    // 使用 @PostConstruct 初始化信号量
    @jakarta.annotation.PostConstruct
    public void initSemaphores() {
        int maxPoolSize = appProperties.getTargetDbConfig().getMaxPoolSize();

        // 1. 计算 Load 并发度
        int configLoad = appProperties.getExecutor().getLoadConcurrency();
        int finalLoad = (configLoad > 0) ? configLoad : (int)(maxPoolSize * 0.6); // 默认占 60% 连接
        resizeLoadPermits(Math.max(1, finalLoad));

        // 2. 计算 Verify 并发度
        int configVerify = appProperties.getExecutor().getVerifyConcurrency();
        int finalVerify = (configVerify > 0) ? configVerify : (int)(maxPoolSize * 0.3); // 默认占 30% 连接
        resizeVerifyPermits(Math.max(1, finalVerify));
    }

    /**
     * 【新增】动态调整 Load 任务并发度
     */
    public synchronized void resizeLoadPermits(int newPermits) {
        if (newPermits <= 0) return;
        log.info("动态调整 Load 并发度: {} -> {}", this.currentLoadLimit, newPermits);
        this.currentLoadLimit = newPermits;
        // 直接替换 Semaphore。
        // 旧的 Semaphore 会被正在运行的任务持有并在结束后释放(虽然没用了)，
        // 新的任务会获取新的 Semaphore。这是安全的。
        this.loadSemaphore = new Semaphore(newPermits);
    }

    /**
     * 【新增】动态调整 Verify 任务并发度
     */
    public synchronized void resizeVerifyPermits(int newPermits) {
        if (newPermits <= 0) return;
        log.info("动态调整 Verify 并发度: {} -> {}", this.currentVerifyLimit, newPermits);
        this.currentVerifyLimit = newPermits;
        this.verifySemaphore = new Semaphore(newPermits);
    }

    public int getCurrentLoadLimit() {
        return currentLoadLimit;
    }

    public int getCurrentVerifyLimit() {
        return currentVerifyLimit;
    }

    // 1. 转码专用线程池 (CPU密集型，保持使用传统线程池)
    @Bean("transcodeExecutor")
    public Executor transcodeExecutor() {
        // CPU 密集型任务，线程数由配置文件控制，不宜过大
        return buildExecutor(appProperties.getExecutor().getTranscode(), "Transcode-");
    }

    // 2. 装载专用线程池 (IO密集型，升级为虚拟线程)
    @Bean("loadExecutor")
    public Executor loadExecutor() {
        // Java 21 虚拟线程：为每个任务创建一个新的轻量级线程
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    // 3. 验证专用线程池 (IO密集型，升级为虚拟线程)
    @Bean("verifyExecutor")
    // Java 21 虚拟线程
    public Executor verifyExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    // 通用构建方法
    private ThreadPoolTaskExecutor buildExecutor(AppProperties.ExecutorConfig config, String prefix) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(config.getCoreSize());
        executor.setMaxPoolSize(config.getMaxSize());
        executor.setQueueCapacity(config.getQueueCapacity());
        executor.setThreadNamePrefix(prefix);
        // 拒绝策略：调用者运行 (防止队列满了丢任务，而是让调度线程自己跑，变相减缓调度速度)
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

    @Autowired private Executor transcodeExecutor;
    @Autowired private Executor loadExecutor;
    @Autowired private Executor verifyExecutor;

    // 机器 A 只抢占 机器 A 的任务
    // --- 2. 调度逻辑 (每 5 秒轮询) ---
    @Scheduled(fixedDelay = 5000)
    public void schedule() {
        // 如果当前没有任何一个作业处于 MIGRATING 状态，直接跳过
        // 这是一个极快的 Count 查询，比 SKIP LOCKED 轻量得多
        if (jobRepo.countByStatus(JobStatus.ACTIVE) == 0) {
            log.trace("No jobs in active");
            return;
        }

        String myIp = appProperties.getCurrentNodeIp(); // 从配置获取本机 IP
        // 防御性检查：如果没配，抛异常或者打印严重警告
        if (myIp == null || myIp.isEmpty()) {
            log.error("严重错误：未配置 app.current-node-ip，调度器无法工作！");
            return;
        }

        // 只有当有活动作业时，才去执行繁重的任务抢占逻辑
        // Modify to use 'self' to trigger Transactional proxy
        self.dispatchTranscode(myIp);
        self.dispatchLoad(myIp);
        self.dispatchVerify(myIp);
    }

    /**
     * 机器 A 只负责救援 机器 A 的僵尸任务
     * 每 10 分钟运行一次
     * 检查那些"卡住"超过 30 分钟的任务
     */
    @Scheduled(fixedDelay = 600000)
    public void rescueStuckTasks() {
        String myIp = appProperties.getCurrentNodeIp();

        // 1. 捞出处于 LOADING 状态 且 最后更新时间早于 30分钟前 的任务
        int count = splitRepo.resetZombieTasksDirectly(CsvSplitStatus.LOADING.toString(), CsvSplitStatus.WAIT_LOAD.toString(), myIp);
        if (count > 0) {
            log.error("本机[{}]发现僵尸任务 Split[{}]，卡在 LOADING 超过30分钟，强制重置。", myIp);
        }

        // 2. 捞出处于 VERIFYING 状态 且 最后更新时间早于 30分钟前 的任务
        count = splitRepo.resetZombieTasksDirectly(CsvSplitStatus.VERIFYING.toString(), CsvSplitStatus.WAIT_VERIFY.toString(), myIp);
        if (count > 0) {
            log.error("本机[{}]发现僵尸任务 Split[{}]，卡在 VERIFYING 超过30分钟，强制重置。", myIp);
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
                // 3. 【修复】注册事务同步回调，确保事务提交后再触发异步任务
                // 防止 Worker 线程启动太快，事务还没提交，导致 Worker 读到旧状态而退出
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        transcodeExecutor.execute(() -> {
                            transcodeService.execute(d.getId());
                        });
                    }
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
        // 0. 快速检查：如果信号量已满，直接跳过数据库查询，减轻 DB 压力
        if (loadSemaphore.availablePermits() <= 0) {
            log.trace("Load 线程池已满，跳过本次调度");
            return;
        }

        // 注意：这里可以一次取更多，因为装载通常比转码快
        // 但不要超过剩余的信号量许可数，避免取出后因为抢不到锁而空跑
        // 这里简单处理，还是取 Top20，依靠后续 tryAcquire 控制
        List<CsvSplit> splits = splitRepo.findAndLockTop20ByStatusAndNodeId(CsvSplitStatus.WAIT_LOAD.toString(), myIp);
        for (CsvSplit s : splits) {
            if (inFlightSplits.contains(s.getId())) continue; // 防抖

            // 1. 尝试获取信号量许可
            if (!loadSemaphore.tryAcquire()) {
                // 如果拿不到许可，说明并发已满，停止后续调度，等待下次轮询
                break;
            }

            // 2. 【核心修改】直接调用 Repo 更新，不走 StateManager
            // 这里的逻辑极快，纯 DB 操作，完全在一个事务内
            int rows = splitRepo.updateStatus(s.getId(), CsvSplitStatus.WAIT_LOAD, CsvSplitStatus.LOADING);
            if (rows > 0) {
                // 更新成功，内存加锁
                inFlightSplits.add(s.getId());

                // 3. 【修复】同样使用事务同步
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        loadExecutor.execute(() -> {
                            try {
                                loadService.execute(s.getId());
                            } finally {
                                inFlightSplits.remove(s.getId());
                                // 【关键】任务结束释放信号量
                                loadSemaphore.release();
                            }
                        });
                    }
                });
            } else {
                // 更新数据库失败（可能被别人抢了），立即释放刚才拿到的信号量
                loadSemaphore.release();
            }

        }
    }

    // --- 阶段 3: 调度校验 (针对 Split) ---
    @Transactional // 必须开启事务
    public void dispatchVerify(String myIp) {
        if (verifySemaphore.availablePermits() <= 0) {
            return;
        }

        // SELECT * FROM t_split WHERE status = 'WAIT_VERIFY' LIMIT 20
        List<CsvSplit> splits = splitRepo.findAndLockTop20ByStatusAndNodeId(CsvSplitStatus.WAIT_VERIFY.toString(), myIp);
        for (CsvSplit s : splits) {
            if (inFlightSplits.contains(s.getId())) continue;

            if (!verifySemaphore.tryAcquire()) {
                break;
            }

            int rows = splitRepo.updateStatus(s.getId(), CsvSplitStatus.WAIT_VERIFY, CsvSplitStatus.VERIFYING);
            if (rows > 0) {
                inFlightSplits.add(s.getId());

                // 3. 【修复】同样使用事务同步
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        verifyExecutor.execute(() -> {
                            try {
                                verifyService.execute(s.getId());
                            } finally {
                                inFlightSplits.remove(s.getId());
                                // 【关键】释放信号量
                                verifySemaphore.release();
                            }
                        });
                    }
                });
            } else {
                verifySemaphore.release();
            }
        }
    }
}