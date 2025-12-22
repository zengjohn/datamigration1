package com.example.moveprog.repository;

import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.DetailStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;

/**
 * 迁移明细(一个明细对于一个csv文件)
 */
@Repository
public interface QianyiDetailRepository extends JpaRepository<QianyiDetail, Long> {

    /**
     * 查询特定状态的任务
     * 用途: MigrationDispatcher 抓取 "NEW" (待转码) 的任务
     */
    List<QianyiDetail> findByStatus(DetailStatus status);

    /**
     * 查询处于多个状态之一的任务
     * 用途: MigrationDispatcher 抓取 "WAIT_LOAD" 和 "WAIT_RELOAD" 的任务
     */
    List<QianyiDetail> findByStatusIn(Collection<DetailStatus> statuses);

    /**
     * 统计某个批次下，未达到指定状态的任务数量
     * 用途: MigrationDispatcher 检查批次是否完成 (countBy...StatusNot(id, "PASS") == 0 则代表全完成了)
     */
    long countByQianyiIdAndStatusNot(Long qianyiId, DetailStatus status);

    /**
     * 查询状态为指定值的前5条记录
     * findTop5ByStatus：按规则推导SQL
     * - findTop5：取前5条
     * - ByStatus：按status字段过滤
     * @param status 状态枚举
     * @return 前5条符合条件的记录
     */
    List<QianyiDetail> findTop5ByStatus(DetailStatus status);

    List<QianyiDetail> findByJobId(Long jobId);

}