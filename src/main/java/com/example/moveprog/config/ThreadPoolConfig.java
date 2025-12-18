package com.example.moveprog.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class ThreadPoolConfig {

    // 定义一个名为 "dbLoadExecutor" 的专用线程池
    @Bean("dbLoadExecutor") 
    public Executor dbLoadExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 核心线程数：建议设置为数据库允许的最大并发连接数的一半，或者根据 CPU 核数 * 2
        executor.setCorePoolSize(5);
        // 最大线程数：不要太大，防止数据库连接池耗尽
        executor.setMaxPoolSize(10);
        // 队列容量：允许有多少个切分文件在排队
        executor.setQueueCapacity(200);
        // 线程名称前缀，方便看日志排查问题
        executor.setThreadNamePrefix("DB-Loader-");
        // 拒绝策略：如果队列满了，由调用者线程（调度器线程）自己去执行，起到限流作用
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}