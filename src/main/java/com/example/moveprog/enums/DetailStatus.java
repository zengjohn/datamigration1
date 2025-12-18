package com.example.moveprog.enums;

public enum DetailStatus {
    // --- 阶段 1: 转码 ---
    NEW,             // 新建 (监视器扫描到)
    TRANSCODING,     // 转码中 (TranscodeService 正在运行)
    FAIL_TRANSCODE,  // 转码失败 (严重错误，如文件损坏)

    // --- 阶段 2: 装载 ---
    WAIT_LOAD,       // 待装载 (转码完成，所有 Split 已生成)
    LOADING,         // 装载中 (LoadService 正在提交并发任务)
    WAIT_RELOAD,     // 待重试 (断点续传状态：有部分 Split 失败，等待人工或自动重试)
    FAIL_LOAD,       // 装载系统故障 (如数据库连不上，不是数据问题)

    // --- 阶段 3: 验证与完成 ---
    WAIT_VERIFY,     // 待验证 (所有 Split 都 PASS，等待核对行数/金额)
    PASS             // 全部完成 (最终成功状态)
}