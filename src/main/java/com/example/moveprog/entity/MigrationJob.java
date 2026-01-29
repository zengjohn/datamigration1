package com.example.moveprog.entity;

import com.example.moveprog.enums.JobStatus;
import jakarta.persistence.*;
import lombok.Data;

/**
 * 迁移作业 主表
 *    1. 源目录(ok文件保存的文件夹)
 *    2. 目标库(csv文件转储tdsql目标库的配置)
 */
@Entity
@Data
@Table(name = "migration_job", indexes = {
        @Index(name = "idx_status", columnList = "status")       // 加速状态过滤
})
public class MigrationJob {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 作业名称，作业的唯一标识 (unique)
     */
    @Column(nullable = false, length = 100)
    private String name;

    /**
     * ok文件保存的目录(文件夹)
     */
    @Column(nullable = false, length = 255)
    private String sourceDirectory;       // 监视目录

    /**
     * 输出文件目录(工作目录)
     */
    @Column(nullable = false, length = 255)
    private String outDirectory;

    /**
     * csv迁移到目标库的配置
     */
    @Column(nullable = false, length = 1000)
    private String targetDbUrl;
    @Column(nullable = false, length = 40)
    private String targetDbUser;
    @Column(length = 40)
    private String targetDbPass;

    /**
     * 是否启用
     */
    @Enumerated(EnumType.STRING)
    private JobStatus status;

}