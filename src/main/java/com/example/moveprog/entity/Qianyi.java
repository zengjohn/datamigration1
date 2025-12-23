package com.example.moveprog.entity;

import com.example.moveprog.enums.BatchStatus;
import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

/**
 * 对应一个ok文件
 *   1. 对应一个Qianyi
 *   2. 包括一个schema定义文件路径(schema文件名就是表名)，一个或者多个待处理的IBM1388 csv文件路径
 */
@Entity
@Data
@Table(name = "qianyi")
public class Qianyi {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 关联哪个配置
     * 相当于MigrationJob表id的外键引用
     */
    @Column(name = "job_id", nullable = false)
    private Long jobId;

    /**
     * OK文件路径
     */
    @Column(nullable = false)
    private String okFilePath;
    /**
     * 目标端表名 （目前的设计，要求一个ok文件中所有的csv文件都装载到同一张表)
     */
    @Column(nullable = false)
    private String tableName;
    /**
     * schema文件路径
     */
    @Column(nullable = false)
    private String ddlFilePath;

    /**
     * 状态
     */
    @Enumerated(EnumType.STRING)
    private BatchStatus status;

    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    @PrePersist void onCreate() { createTime = LocalDateTime.now(); updateTime = LocalDateTime.now(); }
    @PreUpdate void onUpdate() { updateTime = LocalDateTime.now(); }
}