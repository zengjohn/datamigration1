package com.example.moveprog.enums;

public enum CsvSplitStatus {
    /**
     * 待装载 (初始状态)
     */
    WL,

    /**
     * 待验证
     */
    WV,

    /**
     * 验证通过
     */
    PASS,

    /**
     * 失败
     */
    FAIL

}