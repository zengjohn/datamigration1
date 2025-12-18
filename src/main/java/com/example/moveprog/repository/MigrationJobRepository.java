package com.example.moveprog.repository;

import com.example.moveprog.entity.MigrationJob;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 迁移作业
 */
@Repository
public interface MigrationJobRepository extends JpaRepository<MigrationJob, Long> {

    /**
     * 查询所有已启用的作业配置
     * 用途: DirectoryMonitor 扫描时只处理 active=true 的目录
     */
    List<MigrationJob> findByActiveTrue();


    // 1. 使用实体类名 MigrationJob (区分大小写)
    // 2. 返回值改成 Boolean，因为 select m.active 只查这一个字段
    @Query("select m.active from MigrationJob m where m.id = :id")
    Boolean findActiveStatusById(@Param("id") Long id);

}