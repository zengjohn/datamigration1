package com.example.moveprog.dto;

import lombok.Data;

@Data
public class GlobalVerifyResult {
    // 表名 (例如: test2.user_table)
    private String tableName;

    // 状态: MATCH, MISMATCH_SPLIT, MISMATCH_LOAD, ERROR
    private String status;

    // 源文件行数 (IBM)
    private Long sourceRowCount;

    // 拆分文件行数 (UTF-8)
    private Long splitRowCount;

    // 数据库实际行数
    private Long targetRowCount;
}