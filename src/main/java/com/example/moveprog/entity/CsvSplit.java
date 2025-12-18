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

    private Long detailId; // FK -> QianyiDetail

    private String splitFilePath; // 切分后的 UTF8 文件
    private Long rowCount;        // 行数

    @Enumerated(EnumType.STRING)
    private CsvSplitStatus status; // 改用枚举
    
    @Column(columnDefinition = "TEXT")
    private String errorMsg;
}