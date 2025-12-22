package com.example.moveprog.enums;

public enum DetailStatus {
    NEW,             // 刚扫描到，需要转码
    TRANSCODING,     // 正在转码/拆分中
    FAIL_TRANSCODE,  // 转码失败

    PROCESSING_CHILDS,  // 【重要】拆分已完成，正在处理子任务。
                        // (此时 Detail 不再参与调度，而是在等待 Split 变更为 PASS)
    FINISHED, // 所有子切片都 PASS
    FINISHED_WITH_ERROR // 所有子切片跑完，但有失败
}