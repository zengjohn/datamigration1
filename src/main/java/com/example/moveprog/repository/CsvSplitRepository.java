package com.example.moveprog.repository;

import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.enums.CsvSplitStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 一个大机csv文件拆分成多个用于并发装载的csv文件
 */
@Repository
public interface CsvSplitRepository extends JpaRepository<CsvSplit, Long>, JpaSpecificationExecutor<CsvSplit> {

    //List<CsvSplit> findByDetailIdAndStatus(Long detailId, CsvSplitStatus status);

    List<CsvSplit> findByDetailId(Long detailId);

    /**
     * 统计某源文件任务下，未成功的切分文件数量
     * 用途: LoadService 判断整个 Detail 是否可以进入下一阶段 (PASS 或 WAIT_VERIFY)
     */
    //long countByDetailIdAndStatusNot(Long detailId, CsvSplitStatus status);

    // --- 【新增】给 Dispatcher 用的方法 ---
    // 抢占装载/验证任务
    @Query(value = """
        SELECT * FROM csv_split 
        WHERE status = :status 
        AND node_id = :nodeId
        LIMIT 20 
        FOR UPDATE SKIP LOCKED 
        """, nativeQuery = true)
    List<CsvSplit> findAndLockTop20ByStatusAndNodeId(
            @Param("status") String status,
            @Param("nodeId") String nodeId);

    // 逻辑：直接查找并更新那些 status='LOADING' 且 update_time 早于 (NOW - 30分钟) 的记录
    // 僵尸任务检测
    // 这样根本不需要把数据查回 Java 做减法，彻底规避时区问题
    @Modifying
    @Transactional
    @Query(value = """
        UPDATE csv_split  
        SET status = :newStatus, error_msg = '僵尸任务重置(DB判定)' 
        WHERE status = :oldStatus  
        AND node_id = :nodeId 
        AND update_time < DATE_SUB(NOW(), INTERVAL 30 MINUTE)
        """, nativeQuery = true)
    int resetZombieTasksDirectly(
            @Param("oldStatus") String oldStatus,
            @Param("newStatus") String newStatus,
            @Param("nodeId") String nodeId);

    /**
     * 用途: 如果需要重置整个转码过程，可能需要先清理旧的切分记录
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM CsvSplit s WHERE s.detailId = :detailId")
    void deleteByDetailId(Long detailId);

    @Modifying
    @Transactional
    @Query("DELETE FROM CsvSplit s WHERE s.qianyiId = :qianyiId")
    void deleteByQianyiId(Long qianyiId);

    //List<CsvSplit> findTop20ByStatus(CsvSplitStatus status);
    //List<CsvSplit> findByJobId(Long jobId);

    @Modifying
    @Transactional
    @Query("UPDATE CsvSplit s SET s.status = :newStatus WHERE s.status = :oldStatus and s.nodeId = :nodeId")
    int resetStatus(
            @Param("oldStatus") CsvSplitStatus oldStatus,
            @Param("newStatus") CsvSplitStatus newStatus,
            @Param("nodeId") String nodeId);

    /**
     * 【新增】调度器专用：极速更新状态
     * 只有当当前状态符合预期时才更新 (CAS 乐观锁思想，双重保险)
     */
    @Modifying
    @Query("UPDATE CsvSplit s SET s.status = :newStatus WHERE s.id = :id AND s.status = :oldStatus")
    @Transactional // 【核心修复】加上这个，确保该方法自带事务运行
    int updateStatus(
            @Param("id") Long id,
            @Param("oldStatus") CsvSplitStatus oldStatus,
            @Param("newStatus") CsvSplitStatus newStatus);

    @Modifying
    @Transactional
    @Query("UPDATE CsvSplit s SET s.status = 'WAIT_LOAD', s.errorMsg = NULL " +
            "WHERE s.detailId = :detailId AND s.status = 'FAIL_LOAD'")
    int resetFailedSplitsToWaitLoad(Long detailId);

    @Modifying
    @Transactional
    @Query("UPDATE CsvSplit s SET s.status = 'WAIT_VERIFY', s.errorMsg = NULL " +
            "WHERE s.detailId = :detailId AND s.status = 'FAIL_VERIFY'")
    int resetFailedSplitsToWaitVerify(Long detailId);

    // 【查询3】统计拆分切片总行数
    @Query("""
        SELECT SUM(s.rowCount) 
        FROM CsvSplit s 
        JOIN QianyiDetail d ON s.detailId = d.id
        WHERE s.jobId = :jobId
        AND d.targetSchema = :schema
        AND d.targetTableName = :tableName
        """)
    Long sumSplitRows(@Param("jobId") Long jobId,
                      @Param("schema") String schema,
                      @Param("tableName") String tableName);

}