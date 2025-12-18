package com.example.moveprog.repository;

import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.enums.CsvSplitStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

/**
 * 一个大机csv文件拆分成多个用于并发装载的csv文件
 */
@Repository
public interface CsvSplitRepository extends JpaRepository<CsvSplit, Long> {

    /**
     * 查询某源文件任务下，所有未成功的切分文件
     * 用途: LoadService 获取待装载列表 (支持断点续传，自动跳过 PASS 的)
     */
    List<CsvSplit> findByDetailIdAndStatusNot(Long detailId, CsvSplitStatus status);

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
}