package com.example.moveprog.dto;

import lombok.Data;
import java.util.Map;

/**
 * 运维看板数据对象
 */
@Data
public class JobDashboardDto {
    private Long jobId;
    
    // 核心进度 (0-100)
    private int transcodeProgress; // 转码进度
    private int loadProgress;      // 装载进度
    private int verifyProgress;    // 校验进度

    // 状态计数 (红绿灯数据源)
    // Key: 状态名 (如 FAIL_LOAD), Value: 数量
    private Map<String, Long> statusCounts; 
    
    // 错误摘要 (Top 5 报错原因)
    // Key: 错误简述, Value: 出现次数
    private Map<String, Long> topErrors; 
}