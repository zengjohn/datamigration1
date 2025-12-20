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
        // 1. 校验列数
        if (dbRow.length != fileRow.length) {
            throw new RuntimeException("列数不一致");
        }

        // 2. 先校验行号 (数组最后一个元素)
        String dbRowNum = dbRow[dbRow.length - 1];
        String fileRowNum = fileRow[fileRow.length - 1];

        if (!dbRowNum.equals(fileRowNum)) {
            // 这是最关键的报错：说明发生了错位！
            // 比如 DB 是行号3，文件读到了行号2(如果不跳空行)或者行号4
            throw new RuntimeException(String.format(
                    "行号对齐失败 (错位): DB行号=%s, 文件行号=%s", dbRowNum, fileRowNum
            ));
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