package com.example.moveprog.entity;

import com.example.moveprog.enums.CsvSplitStatus;
import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 装载/验证任务表
 * 拆分切片 - csv(utf8编码)
 *   一个IBM1388 csv文件，为了支持并发装载， 会被转码拆分成多个utf8文件
 */
@Entity
@Data
@Table(name = "csv_split", indexes = {
        @Index(name = "idx_job_id", columnList = "job_id"),
        @Index(name = "idx_qianyi_id", columnList = "qianyi_id"),
        @Index(name = "idx_detail_id", columnList = "detail_id"), // 加速 findByDetailId
        @Index(name = "idx_status_node", columnList = "status, node_id")       // 加速状态过滤
})
public class CsvSplit extends BaseNodeEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id; // 这就是目标表里的 csvid

    @Column(name = "job_id", nullable = false)
    private Long jobId;    // 冗余字段，加索引

    @Column(name = "qianyi_id", nullable = false)
    private Long qianyiId;

    /**
     * 被拆分的明细id (归属父文件)
     * FK -> QianyiDetail
     */
    @Column(name = "detail_id", nullable = false)
    private Long detailId;

    /**
     * 切分后的 UTF8 文件
     * 注意这里保存的是全路径名,  例如 /outdir/10_1.csv
     */
    @Column(nullable = false, length = 255)
    private String splitFilePath;

    /**
     * 拆分文件第一行在原文件中的行数
     */
    @Column(nullable = false)
    private Long startRowNo;
    /**
     * 行数
     */
    @Column(nullable = false)
    private Long rowCount;

    /**
     * 状态
     */
    @Column(name = "status", length = 20) // 数据库字段定义为 varchar(20)
    @Enumerated(EnumType.STRING)    // 【关键】告诉 JPA 存枚举的名字
    private CsvSplitStatus status;
    
    @Column(columnDefinition = "TEXT")
    private String errorMsg;

    @Column(name = "update_time",
            // 【关键1】DDL 定义：告诉 Hibernate 建表时用什么 SQL
            columnDefinition = "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            // 【关键2】禁止 Java 写入：JPA 在生成 insert/update 语句时会忽略这个字段
            insertable = false,
            updatable = false)
    // 【关键】自动刷新：告诉 Hibernate，每次插入或更新后，立刻从 DB 读回最新的时间
    // 这样你在 save() 之后，立即 getUpdateTime() 也能拿到正确的时间
    @org.hibernate.annotations.Generated(org.hibernate.annotations.GenerationTime.ALWAYS)
    private LocalDateTime updateTime;

}