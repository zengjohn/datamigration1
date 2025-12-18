package com.example.moveprog.enums;

public enum CsvSplitStatus {
    WL,    // Wait Load - 待装载 (初始状态)
    PASS,  // Success - 装载成功
    FAIL   // Failure - 装载失败 (主键冲突以外的错误，如数据类型不匹配)
}