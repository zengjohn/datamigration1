package com.example.moveprog.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

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

    // 有时间时在将下面的按类型比对合并进来

    /**
     * 单个单元格比对逻辑
     * @param dbStr 数据库取出的值 (通常已经通过 ResultSet.getString 转好了)
     * @param fileStr CSV文件里的值
     * @param sqlType java.sql.Types 的类型
     */
    protected boolean isCellEqual(String dbStr, String fileStr, int sqlType) {
        String val1 = (dbStr == null) ? "" : dbStr.trim();
        String val2 = (fileStr == null) ? "" : fileStr.trim();

        if (val1.equals(val2)) return true;
        if (val1.isEmpty() && val2.isEmpty()) return true;

        switch (sqlType) {
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TIME:
                return compareTimestamp(val1, val2);

            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.DOUBLE:
                return compareNumber(val1, val2);

            default:
                return false;
        }
    }

    // 定义多种可能的时间格式，用于解析 CSV 字符串
    // 这种带 [.SSSSSS] 的写法是关键，可选匹配微秒
    private static final DateTimeFormatter[] DATE_FORMATS = {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSSSSS") // 兼容斜杠
    };

    /**
     * 【核心】高精度时间比对
     * 解决：
     * 1. DB: 2023-01-01 10:00:00.000000 vs CSV: 2023-01-01 10:00:00 (缺省)
     * 2. DB: 2023-01-01 10:00:00.123456 vs CSV: 2023-01-01 10:00:00.123456
     */
    private boolean compareTimestamp(String v1, String v2) {
        try {
            Timestamp t1 = parseTimestamp(v1);
            Timestamp t2 = parseTimestamp(v2);

            // 使用 compareTo == 0 来比较，能够正确处理 nanos
            return t1.compareTo(t2) == 0;

        } catch (Exception e) {
            // 如果解析失败，说明格式严重不符，直接返回 false
            // 或者 log.warn("时间格式解析失败: {} vs {}", v1, v2);
            return false;
        }
    }

    private Timestamp parseTimestamp(String val) {
        // 1. 尝试直接用 Timestamp.valueOf (格式必须是 yyyy-mm-dd hh:mm:ss[.f...])
        try {
            return Timestamp.valueOf(val);
        } catch (IllegalArgumentException e) {
            // 忽略，继续尝试
        }

        // 2. 尝试手动归一化 (补全微秒)
        // 有时候 DB 返回 .0 而 CSV 没有，或者反之
        // 最暴力的办法：都转成纳秒级对比，或者利用 DateTimeFormatter
        for (DateTimeFormatter fmt : DATE_FORMATS) {
            try {
                // 如果是 LocalDateTime 转换
                java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(val, fmt);
                return Timestamp.valueOf(ldt);
            } catch (DateTimeParseException ignored) {}
        }

        throw new RuntimeException("Unparseable date: " + val);
    }

    private boolean compareNumber(String v1, String v2) {
        try {
            return new BigDecimal(v1).compareTo(new BigDecimal(v2)) == 0;
        } catch (Exception e) {
            return false;
        }
    }

}