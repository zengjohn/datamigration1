package com.example.moveprog.entity;

import com.example.moveprog.enums.BatchStatus;
import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "qianyi")
public class Qianyi {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long jobId;         // 关联哪个配置
    private String tableName;   // 从 DDL 路径解析出的表名
    private String okFilePath;  // 触发文件路径
    private String ddlFilePath; // DDL路径

    @Enumerated(EnumType.STRING)
    private BatchStatus status;

    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    @PrePersist void onCreate() { createTime = LocalDateTime.now(); updateTime = LocalDateTime.now(); }
    @PreUpdate void onUpdate() { updateTime = LocalDateTime.now(); }
}