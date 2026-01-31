package com.example.moveprog.entity;

import com.example.moveprog.enums.BatchStatus;
import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

/**
 * 批次表
 * 对应一个ok文件
 *   1. 对应一个Qianyi
 *   2. 包括一个schema定义文件路径(schema文件名就是表名)，一个或者多个待处理的IBM1388 csv文件路径
 */
@Entity
@Data
@Table(name = "qianyi", indexes = {
        @Index(name = "idx_status_node", columnList = "status, node_id")       // 加速状态过滤
})
public class Qianyi extends BaseNodeEntity {
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
    @Column(nullable = false, length = 255)
    private String okFilePath;

    /**
     * 目标库名, 如果为空则库名由url决定
     */
    @Column(nullable = true, length = 255)
    private String schemaName;


    /**
     * 目标端表名 （目前的设计，要求一个ok文件中所有的csv文件都装载到同一张表)
     */
    @Column(nullable = false, length = 255)
    private String tableName;

    /**
     * schema文件路径
     */
    @Column(nullable = false, length = 255)
    private String ddlFilePath;

    /**
     * 状态
     */
    @Enumerated(EnumType.STRING)
    private BatchStatus status;

    // 【新增】记录解析失败的原因
    @Column(columnDefinition = "TEXT")
    private String errorMsg;

    @Column(name = "create_time", insertable = false, updatable = false,
           columnDefinition = "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    @org.hibernate.annotations.Generated(org.hibernate.annotations.GenerationTime.ALWAYS)
    private LocalDateTime createTime;

    @Column(name = "update_time", insertable = false, updatable = false,
            columnDefinition = "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    @org.hibernate.annotations.Generated(org.hibernate.annotations.GenerationTime.ALWAYS)
    private LocalDateTime updateTime;
}