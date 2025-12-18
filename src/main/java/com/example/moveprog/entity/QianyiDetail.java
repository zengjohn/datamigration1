package com.example.moveprog.entity;

import com.example.moveprog.enums.DetailStatus;
import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "qianyi_detail")
public class QianyiDetail {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long qianyiId;      // 归属于哪个批次
    private String tableName;   // 冗余一下表名，方便查询
    private String sourceCsvPath; // 源 CSV 路径 (JSON里的一个条目)

    // 状态机: NEW -> TRANSCODING -> WAIT_LOAD -> LOADING -> WAIT_VERIFY -> PASS
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