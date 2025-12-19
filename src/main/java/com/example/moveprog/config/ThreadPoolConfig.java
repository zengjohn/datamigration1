package com.example.moveprog.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableAsync
public class ThreadPoolConfig {

    // ==========================================
    // 1. 专用线程池：用于 LoadService (密集IO/DB操作)
    // ==========================================
    @Bean("dbLoadExecutor")
    public Executor dbLoadExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 核心线程数：根据数据库连接池大小设定，比如 10-20
        executor.setCorePoolSize(10);
        // 最大线程数
        executor.setMaxPoolSize(20);
        // 队列容量：缓冲队列
        executor.setQueueCapacity(200);
        // 线程名前缀，方便查日志
        executor.setThreadNamePrefix("DB-Load-Exec-");
        // 拒绝策略：由调用者所在的线程执行 (防止任务丢失)
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

    // ==========================================
    // 2. 通用/默认线程池：用于普通 @Async (消除警告)
    // ==========================================
    @Bean("taskExecutor") // Spring 默认找这个名字
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // CPU 核数 + 1 或者是较小的数值，处理杂项异步任务
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("General-Async-");
        executor.initialize();
        return executor;
    }

    /**
     * 比对专用线程池
     * 特点：IO 密集型（大量读库），但也消耗 CPU（大量字符串比对）
     * 建议核心线程数不要超过 DB 连接池大小的 1/2
     */
    @Bean("verifyExecutor")
    public Executor verifyExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 核心线程数：建议 5 ~ 10，太高会把数据库连接池打满
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(200);
        executor.setThreadNamePrefix("verify-exec-");
        // 拒绝策略：调用者运行（防止任务丢失，起到削峰作用）
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

}