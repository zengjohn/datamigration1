package com.example.moveprog.entity;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import lombok.Data;

@Data
@MappedSuperclass // 关键注解：表示这不是一张表，但它的字段会被子类继承到表中
public abstract class BaseNodeEntity {

    /**
     * 节点标识 (IP 或 机器ID)
     * 建立索引建议: ALTER TABLE xxx ADD INDEX idx_status_node (status, node_id);
     */
    @Column(name = "node_id", length = 50)
    private String nodeId;
}