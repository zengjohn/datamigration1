package com.example.moveprog.enums;

// 定义比对策略枚举
public enum VerifyStrategy {
    USE_UTF8_SPLIT,  // 速度快，比对转码后的文件
    USE_SOURCE_FILE  // 溯源，比对原始 IBM1388 文件
}