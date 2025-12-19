package com.example.moveprog.entity;

import com.example.moveprog.enums.CsvSplitStatus;
import jakarta.persistence.*;
import lombok.Data;

@Entity
@Data
@Table(name = "csv_split")
public class CsvSplit {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id; // 这就是目标表里的 csvid

    // 1. 明确定义这个 ID 字段，方便查询
    @Column(name = "detail_id")
    private Long detailId; // FK -> QianyiDetail

    private String splitFilePath; // 切分后的 UTF8 文件
    private Long startRowNo; // 拆分文件第一行在原文件中的行数
    private Long rowCount;        // 行数

    @Enumerated(EnumType.STRING)
    private CsvSplitStatus status; // 改用枚举
    
    @Column(columnDefinition = "TEXT")
    private String errorMsg;
}