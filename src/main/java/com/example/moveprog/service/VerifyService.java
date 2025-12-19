package com.example.moveprog.service;

import com.example.moveprog.entity.*;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.repository.*;
import com.example.moveprog.scheduler.TaskLockManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class VerifyService {

    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;
    private final QianyiRepository qianyiRepo;

    private final TaskLockManager lockManager;

    private final JdbcTemplate jdbcTemplate;

    @Async
    public void execute(Long detailId) {
        QianyiDetail detail = detailRepo.findById(detailId).orElse(null);
        if (detail == null) { lockManager.releaseLock(detailId); return; }

        try {
            log.info("开始校验任务: {}", detailId);
            Qianyi batch = qianyiRepo.findById(detail.getQianyiId()).orElseThrow();
            String tableName = batch.getTableName();

            // 获取该任务下所有 PASS 的切分文件
            List<CsvSplit> splits = splitRepo.findByDetailIdAndStatusNot(detailId, CsvSplitStatus.FAIL);

            for (CsvSplit split : splits) {
                // 如果想要严格比对，必须确保文件是 PASS 的
                if (split.getStatus() != CsvSplitStatus.PASS) {
                    throw new RuntimeException("存在未成功的切分文件，无法校验");
                }

                // 执行流式比对
                verifySingleSplitStream(split, tableName);
            }

            // 全部通过
            detail.setStatus(DetailStatus.PASS);
            detail.setErrorMsg(null);
            detailRepo.save(detail);

        } catch (Exception e) {
            log.error("校验失败", e);
            detail.setStatus(DetailStatus.WAIT_VERIFY); // 保持在待校验
            detail.setErrorMsg("校验不通过: " + e.getMessage());
            detailRepo.save(detail);
        } finally {
            lockManager.releaseLock(detailId);
        }
    }

    /**
     * 流式比对：一边读 CSV，一边读数据库游标，逐行比对内容
     * 利用了索引 (csvid, source_row_no) 避免数据库排序
     */
    private void verifySingleSplitStream(CsvSplit split, String tableName) throws Exception {
        log.info("正在比对文件: {}", split.getSplitFilePath());

        // 1. 构造 SQL，强制按 source_row_no 排序
        // 这一步非常快，因为有联合索引
        String sql = String.format(
            "SELECT user_id, user_name, balance FROM %s WHERE csvid = ? ORDER BY source_row_no", 
            tableName
        );

        // 2. 读取 CSV 文件流
        try (BufferedReader br = Files.newBufferedReader(Paths.get(split.getSplitFilePath()))) {
            
            // 3. 使用 RowCallbackHandler 进行流式处理 (不会一次性把数据加载到内存)
            jdbcTemplate.query(sql, new Object[]{split.getId()}, new RowCallbackHandler() {
                
                // 闭包变量：记录当前 CSV 读到了哪一行
                long dbRowCount = 0;
                boolean verifyFailed = false;

                @Override
                public void processRow(ResultSet rs) throws SQLException {
                    if (verifyFailed) return; // 已经失败就不比了

                    try {
                        String csvLine = br.readLine();
                        if (csvLine == null) {
                            throw new RuntimeException("数据库行数多于CSV文件行数");
                        }
                        
                        // 解析 CSV 行 (注意：切分文件里最后一列是行号，前面是数据)
                        // 假设 CSV: U001,Alice,100.00,1
                        String[] csvParts = csvLine.split(","); // 简单分割，实际建议用 CSVParser
                        
                        // 获取数据库值
                        String dbUserId = rs.getString("user_id");
                        String dbUserName = rs.getString("user_name");
                        // 比较...
                        
                        // 简单核对：比如只核对 ID，或者核对拼接的字符串 hash
                        if (!dbUserId.equals(csvParts[0])) {
                             throw new RuntimeException("内容不一致! Row=" + (dbRowCount+1) + 
                                     " DB=" + dbUserId + " CSV=" + csvParts[0]);
                        }
                        
                        dbRowCount++;
                    } catch (Exception e) {
                        verifyFailed = true;
                        throw new SQLException(e); // 抛出异常中断 query
                    }
                }
            });
            
            // 4. 确认 CSV 是否也读完了
            if (br.readLine() != null) {
                throw new RuntimeException("CSV文件行数多于数据库行数");
            }
        }
    }
}