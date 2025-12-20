package com.example.moveprog.service;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TaskLockManager {

    // 线程安全的 Set
    private final Set<Long> runningTaskIds = ConcurrentHashMap.newKeySet();

    /**
     * 尝试锁定任务
     * @return true 如果锁定成功, false 如果任务已在运行
     */
    public boolean tryLock(Long id) {
        return runningTaskIds.add(id);
    }

    /**
     * 释放任务锁
     */
    public void releaseLock(Long id) {
        runningTaskIds.remove(id);
    }
    
    /**
     * 判断是否锁定中 (可选)
     */
    public boolean isLocked(Long id) {
        return runningTaskIds.contains(id);
    }
}