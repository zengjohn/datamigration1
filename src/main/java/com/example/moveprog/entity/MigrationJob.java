package com.example.moveprog.entity;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Data
@Table(name = "migration_job")
public class MigrationJob {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String jobName;
    private String sourceDir;       // 监视目录
    private String targetJdbcUrl;   // 目标库配置
    private String targetUser;
    private String targetPassword;
    private boolean active;         // 是否启用
}