package com.example.moveprog.enums;

public enum BatchStatus {
    PROCESSING, // 处理中 (只要有一个 Detail 没完，就是这个状态)
    FINISHED    // 已完成 (所有 Detail 都 PASS)
}