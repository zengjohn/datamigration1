package com.example.moveprog.service;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class JobControlManager {

    // Key: JobId, Value: true 表示收到了停止信号
    private final ConcurrentHashMap<Long, Boolean> stopFlags = new ConcurrentHashMap<>();

    /**
     * 发送停止信号 (供 Controller 调用)
     */
    public void stopJob(Long jobId) {
        stopFlags.put(jobId, true);
    }

    /**
     * 恢复/继续任务 (供 Controller 调用)
     * 移除停止信号，这样线程就不再会被拦截
     */
    public void resumeJob(Long jobId) {
        stopFlags.remove(jobId);
    }

    /**
     * 检查是否应该停止 (供 Service 循环中调用)
     * 性能极高，无数据库 I/O
     */
    public boolean shouldStop(Long jobId) {
        return stopFlags.getOrDefault(jobId, false);
    }
}