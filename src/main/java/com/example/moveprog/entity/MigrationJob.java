package com.example.moveprog.entity;

import com.example.moveprog.enums.JobStatus;
import jakarta.persistence.*;
import lombok.Data;

/**
 * 迁移作业
 *    1. 源目录(ok文件保存的文件夹)
 *    2. 目标库(csv文件转储tdsql目标库的配置)
 */
@Entity
@Data
@Table(name = "migration_job")
public class MigrationJob {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 作业名称，作业的唯一标识 (unique)
     */
    private String name;

    /**
     * ok文件保存的目录(文件夹)
     */
    private String sourceDirectory;       // 监视目录

    /**
     * csv迁移到目标库的配置
     */
    private String targetJdbcUrl;
    private String targetUser;
    private String targetPassword;

    /**
     * 是否启用
     */
    @Enumerated(EnumType.STRING)
    private JobStatus status;

}