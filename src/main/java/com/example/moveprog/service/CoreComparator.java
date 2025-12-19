package com.example.moveprog.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CoreComparator {

    /**
     * 核心比对方法 (纯逻辑，不涉及 IO 细节)
     * @param dbIter 数据库数据的迭代器
     * @param fileIter 文件数据的迭代器 (无论是 UTF8 还是 IBM1388)
     */
    public void compareStreams(CloseableRowIterator dbIter, CloseableRowIterator fileIter) {
        long rowCount = 0;

        // 双流同步推进
        while (dbIter.hasNext() && fileIter.hasNext()) {
            rowCount++;
            String[] dbRow = dbIter.next();
            String[] fileRow = fileIter.next();

            // === 这里是你将来需要重写的部分 ===
            doCompare(rowCount, dbRow, fileRow);
            // =================================
        }

        // 校验行数是否一致
        if (dbIter.hasNext()) {
            throw new RuntimeException("校验失败: 数据库行数多于文件 (Row " + (rowCount + 1) + ")");
        }
        if (fileIter.hasNext()) {
            throw new RuntimeException("校验失败: 文件行数多于数据库 (Row " + (rowCount + 1) + ")");
        }
        
        log.info("比对完成，共校验 {} 行", rowCount);
    }

    // 具体的一行比对逻辑
    private void doCompare(long rowNum, String[] dbRow, String[] fileRow) {
        // 假设比对列数以文件为准
        if (dbRow.length != fileRow.length) {
            // 注意：如果DB查询出的列数和CSV不一样，这里需要处理映射关系，或者在SQL里只查特定列
            // throw new RuntimeException("列数不匹配...");
        }

        for (int i = 0; i < fileRow.length; i++) {
            String dbVal = (dbRow[i] == null) ? "" : dbRow[i].trim();
            String fileVal = (fileRow[i] == null) ? "" : fileRow[i].trim();

            if (!dbVal.equals(fileVal)) {
                throw new RuntimeException(String.format(
                    "内容不一致: Row=%d, Col=%d, DB='%s', File='%s'", 
                    rowNum, i + 1, dbVal, fileVal
                ));
            }
        }
    }
}