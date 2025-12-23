package com.example.moveprog.service;

import com.example.moveprog.exception.JobStoppedException;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class JobControlManager {

    // 内存中的“停止名单” (存放已停止的 JobID)
    private Set<Long> stoppedJobIds = ConcurrentHashMap.newKeySet();

    /**
     * 前端点击停止时调用
     */
    public void stopJob(Long jobId) {
        stoppedJobIds.add(jobId);
    }

    /**
     * 前端点击恢复时调用
     */
    public void resumeJob(Long jobId) {
        stoppedJobIds.remove(jobId);
    }

    /**
     * 【核心方法】检查并在需要时抛出异常
     * 这里的逻辑是：如果是停止状态 -> 抛异常 -> 打断业务
     */
    public void checkJobState(Long jobId) {
        if (stoppedJobIds.contains(jobId)) {
            throw new JobStoppedException("作业 [" + jobId + "] 已被人工停止，当前任务中断。");
        }
    }

}