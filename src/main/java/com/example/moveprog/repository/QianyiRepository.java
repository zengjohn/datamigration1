package com.example.moveprog.repository;

import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.enums.BatchStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 对于一个ok(迁移指令或者请求)
 */
@Repository
public interface QianyiRepository extends JpaRepository<Qianyi, Long> {

    /**
     * 检查 ok 文件是否已经处理过
     * 用途: DirectoryMonitor 防止重复生成任务
     */
    boolean existsByOkFilePath(String okFilePath);

    /**
     * 查询特定状态的批次
     * 用途: MigrationDispatcher 检查 "PROCESSING" 的批次是否已经全部完成
     */
    List<Qianyi> findByStatus(BatchStatus status);
}