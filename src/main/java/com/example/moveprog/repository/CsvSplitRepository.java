package com.example.moveprog.repository;

import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.enums.CsvSplitStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 一个大机csv文件拆分成多个用于并发装载的csv文件
 */
@Repository
public interface CsvSplitRepository extends JpaRepository<CsvSplit, Long> {

    List<CsvSplit> findByDetailIdAndStatus(Long detailId, CsvSplitStatus status);

    List<CsvSplit> findByDetailId(Long detailId);

    /**
     * 统计某源文件任务下，未成功的切分文件数量
     * 用途: LoadService 判断整个 Detail 是否可以进入下一阶段 (PASS 或 WAIT_VERIFY)
     */
    long countByDetailIdAndStatusNot(Long detailId, CsvSplitStatus status);
    
    /**
     * (可选) 删除关联的切分记录
     * 用途: 如果需要重置整个转码过程，可能需要先清理旧的切分记录
     */
    void deleteByDetailId(Long detailId);

    @Modifying
    @Query("DELETE FROM CsvSplit s WHERE s.qianyiId = :qianyiId")
    void deleteByQianyiId(Long qianyiId);

    List<CsvSplit> findTop20ByStatus(CsvSplitStatus status);

    List<CsvSplit> findByJobId(Long jobId);

    @Modifying
    @Query("UPDATE CsvSplit s SET s.status = :newStatus WHERE s.status = :oldStatus")
    int resetStatus(CsvSplitStatus oldStatus, CsvSplitStatus newStatus);

    List<CsvSplit> findByStatusAndUpdateTimeBefore(CsvSplitStatus status, LocalDateTime updateTime);

}