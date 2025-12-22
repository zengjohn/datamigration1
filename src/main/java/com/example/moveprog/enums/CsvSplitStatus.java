package com.example.moveprog.enums;

public enum CsvSplitStatus {
    /**
     * 待装载 (初始状态)
     */
    WAIT_LOAD,

    /**
     * 装载失败
     */
    FAIL_LOAD,

    /**
     * 待验证
     */
    WAIT_VERIFY,

    /**
     * 失败
     */
    FAIL_VERIFY,

    /**
     * 验证通过
     */
    PASS

}