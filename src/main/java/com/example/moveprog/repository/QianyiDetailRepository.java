package com.example.moveprog.repository;

import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.DetailStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

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
     * // --- 原有的方法 (留给管理端/Nginx查看总进度用，查所有节点) ---
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

    List<QianyiDetail> findByQianyiId(Long qianyiId);

    List<QianyiDetail> findByJobId(Long jobId);

    //Page<QianyiDetail> findByJobId(Long jobId, Pageable pageable);
    Page<QianyiDetail> findByQianyiId(Long qianyiId, Pageable pageable);


    // --- 【新增】给 Dispatcher 用的方法 (只抢自己节点任务) ---
    /**
     * 抢占转码任务：状态是 NEW 且 NodeId 是本机
     * 【黑科技】查询并锁定任务，自动跳过被锁定的行
     * nativeQuery = true: 必须用原生 SQL 才能写 FOR UPDATE SKIP LOCKED
     */
    @Query(value = """
        SELECT * FROM qianyi_detail 
        WHERE status = :status 
        AND node_id = :nodeId  -- 如果您用了节点绑定，加上这个更安全；没用则去掉
        LIMIT 5 
        FOR UPDATE SKIP LOCKED 
        """, nativeQuery = true)
    List<QianyiDetail> findAndLockTop5ByStatusAndNodeId(
            @Param("status") String status,
            @Param("nodeId") String nodeId);

    // 统计也是一样
    long countByQianyiIdAndStatusNotAndNodeId(Long qianyiId, DetailStatus status, String nodeId);

    // 方法 A: 通过命名规范删除 (Spring Data JPA 自动实现)
    // 只要你的实体里有 qianyiId 这个字段，且是 Long 类型
    //void deleteByQianyiId(Long qianyiId);

    // 方法 B: 如果你想显式控制 SQL (更推荐，性能更好)
    @Modifying
    @Transactional
    @Query("DELETE FROM QianyiDetail d WHERE d.qianyiId = :qianyiId")
    void deleteByQianyiId(Long qianyiId);

    @Modifying
    @Transactional
    @Query("UPDATE QianyiDetail d SET d.status = :newStatus WHERE d.status = :oldStatus")
    int resetStatus(
            @Param("oldStatus") DetailStatus oldStatus,
            @Param("newStatus") DetailStatus newStatus);

    /**
     * 更新转码失败行数
     * * @Transactional(propagation = Propagation.REQUIRES_NEW):
     * 不管当前有没有事务，都开启一个新的独立事务。
     * 这样可以保证：即使 TranscodeService 抛出异常导致外层回滚，
     * 这个错误计数的更新依然会提交保存！
     */
    @Modifying
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Query("UPDATE QianyiDetail d SET d.transcodeErrorCount = :count WHERE d.id = :id")
    void updateErrorCount(Long id, Long count);

    /**
     * 调度器专用
     * @param id
     * @param oldStatus
     * @param newStatus
     * @return
     */
    @Modifying
    @Transactional // <--- 【核心修复】加上这个，确保该方法自带事务运行
    @Query("UPDATE QianyiDetail d SET d.status = :newStatus, d.updateTime = CURRENT_TIMESTAMP WHERE d.id = :id AND d.status = :oldStatus")
    int updateStatus(
            @Param("id") Long id,
            @Param("oldStatus") DetailStatus oldStatus,
            @Param("newStatus") DetailStatus newStatus);

}