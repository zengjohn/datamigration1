package com.example.moveprog.entity;

import com.example.moveprog.enums.DetailStatus;
import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

/**
 * 对应一个IBM1388源csv文件
 */
@Entity
@Data
@Table(name = "qianyi_detail", indexes = {
        @Index(name = "idx_job_id", columnList = "job_id"),
        @Index(name = "idx_qianyi_id", columnList = "qianyi_id"),
        @Index(name = "idx_status", columnList = "status")
})
public class QianyiDetail {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false)
    private Long jobId;    // 冗余字段，加索引

    /**
     * 归属于哪个批次 (相当于外键引用qianyi表id)
     */
    @Column(name = "qianyi_id", nullable = false)
    private Long qianyiId;

    /**
     * 冗余一下表名，方便查询
     */
    private String tableName;
    /**
     * 源 CSV 路径 IBM1388.csv
     */
    private String sourceCsvPath;

    /**
     * 状态
     */
    // 【修改这里】必须加上 @Enumerated(EnumType.STRING)
    // 否则 Hibernate 会以为它是数字 (0, 1, 2...)
    @Enumerated(EnumType.STRING)
    private DetailStatus status;
    
    @Column(columnDefinition = "TEXT")
    private String errorMsg;
    
    private Integer progress; // 0-100

    private LocalDateTime updateTime;
    @PrePersist @PreUpdate void onUpdate() { updateTime = LocalDateTime.now(); }
}