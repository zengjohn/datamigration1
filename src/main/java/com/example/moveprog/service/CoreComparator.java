package com.example.moveprog.service;

import com.example.moveprog.exception.JobStoppedException;
import com.example.moveprog.service.impl.JdbcRowIterator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * 核心比对器 - 修复版
 * 职责：执行具体的行级比对，发现不一致调用 Writer 记录
 */
@Slf4j
@Component
public class CoreComparator {
    @Autowired
    private JobControlManager jobControlManager;

    /**
     * 核心比对方法 (纯逻辑，不涉及 IO 细节)
     * 执行比对流
     * @param dbIter 数据库数据的迭代器
     * @param fileIter 文件数据的迭代器 (无论是 UTF8 还是 IBM1388)
     * @return 返回本次比对发现的差异行数 (用于双重校验)
     */
    public long compareStreams(Long jobId, CloseableRowIterator<String> fileIter, JdbcRowIterator dbIter, VerifyDiffWriter diffWriter) throws IOException {
        // 1. 获取元数据 (用于打印列名)
        int[] colTypes = dbIter.getColumnTypes();
        String[] colNames = dbIter.getColumnNames();

        // 用于统计处理的行数（日志用）
        long processedRows = 0;

        // 2. 初始化缓存行(双指针遍历)
        String[] fileRow = fileIter.hasNext() ? fileIter.next() : null;
        Object[] dbRow = dbIter.hasNext() ? dbIter.next() : null;

        try {
            while (dbRow != null || fileRow != null) {
                processedRows++;

                // 【埋点】每处理 1000 行检查一次
                // 没必要每行都查，太浪费性能；也没必要查太少，响应太慢
                if (processedRows % 1000 == 0) {
                    jobControlManager.checkJobState(jobId);
                }

                // 获取行号 (假设行号在数组最后一位)
                // 注意：需要处理 Long 类型转换
                Long fileRowNo = getRowNo(fileRow);
                Long dbRowNo = getRowNo(dbRow);

                // === Case 1: 源端存在，目标端不存在 (File 有, DB 没有) ===
                // 此时 fileRowNo < dbRowNo (或者 DB 已经读完了)
                if (dbRowNo == null || (fileRowNo != null && fileRowNo < dbRowNo)) {
                    // 源端 !{行号}, 目标端null
                    String msg = String.format("CSV !{%d}, DB: null", fileRowNo);
                    diffWriter.writeDiff(msg);

                    // File 指针后移，DB 不动
                    fileRow = fileIter.hasNext() ? fileIter.next() : null;
                    continue;
                }

                // === Case 2: 源端不存在，目标端存在 (DB 有, File 没有) ===
                // 此时 dbRowNo < fileRowNo (或者 File 已经读完了)
                else if (fileRowNo == null || (dbRowNo != null && dbRowNo < fileRowNo)) {
                    // 源端 null, 目标端 !{行号}
                    String msg = String.format("CSV: null, DB: !{%d}", dbRowNo);
                    diffWriter.writeDiff(msg);

                    // DB 指针后移，File 不动
                    dbRow = dbIter.hasNext() ? dbIter.next() : null;
                    continue;
                }

                // === Case 3: 两端都存在，比较内容 ===
                else {
                    // 3. 行号一致，比对内容
                    // 核心修复：isRowEqual 返回 false 时才去拼接字符串，极大提升性能
                    if (!isRowEqual(dbRow, fileRow, colTypes)) {
                        String diffMsg = formatDiffDetail(dbRow, fileRow, colTypes, colNames, dbRowNo);
                        diffWriter.writeDiff(diffMsg);
                    }

                    // 4. 两个指针都后移
                    dbRow = dbIter.hasNext() ? dbIter.next() : null;
                    fileRow = fileIter.hasNext() ? fileIter.next() : null;
                }
            }

            return diffWriter.getDiffCount().get();
        } catch (JobStoppedException | VerifyDiffWriter.DiffLimitExceededException e) {
            // 【特殊处理】用户叫停
            log.warn("任务被中断: {}", e.getMessage());
            // 此时应该把状态重置回 NEW，或者保持 TRANSCODING 等待下次“启动修复”
            // 建议：不做处理，直接 return。因为下次启动时的 StartupTaskResetter 会负责把 TRANSCODING 重置为 NEW
            return -1;
        }

    }

    /**
     * 快速比对行（不生成垃圾对象）
     */
    private boolean isRowEqual(Object[] dbRow, String[] fileRow, int[] colTypes) {
        // 注意：dbRow 和 fileRow 长度可能包含 row_no，比对时要排除最后一列
        int len = Math.min(dbRow.length, fileRow.length) - 1;

        for (int i = 0; i < len; i++) {
            if (!isCellEqual(dbRow[i], fileRow[i], colTypes[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * 单元格比对（增强健壮性）
     */
    private boolean isCellEqual(Object dbVal, String fileStr, int sqlType) {
        // 1. 空值处理
        boolean dbNull = (dbVal == null);
        boolean fileNull = (fileStr == null || fileStr.isEmpty() || "null".equalsIgnoreCase(fileStr));

        if (dbNull && fileNull) return true;
        if (dbNull || fileNull) return false;

        String dbStr = String.valueOf(dbVal);
        String csvVal = fileStr.trim(); // CSV 读取通常需要 trim

        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                // 数据库取出的定长 CHAR 可能带空格，需要 trim
                return dbStr.trim().equals(csvVal);

            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
                // 【核心修复】数值比较使用 BigDecimal，解决 1.00 != 1.0 的问题
                try {
                    BigDecimal d1 = new BigDecimal(dbStr);
                    BigDecimal d2 = new BigDecimal(csvVal);
                    return d1.compareTo(d2) == 0;
                } catch (Exception e) {
                    return false;
                }

            case Types.TIMESTAMP:
            case Types.DATE:
                // 时间比较建议归一化为字符串或 Timestamp 对象
                // 简单处理：截取前19位 (yyyy-MM-dd HH:mm:ss) 忽略毫秒差异，视业务需求而定
                if (dbStr.length() > 19) dbStr = dbStr.substring(0, 19);
                if (csvVal.length() > 19) csvVal = csvVal.substring(0, 19);
                return dbStr.equals(csvVal);

            default:
                return dbStr.equals(csvVal);
        }
    }

    private String formatDiffDetail(Object[] dbRow, String[] fileRow, int[] types, String[] names, Long rowNo) {
        StringBuilder sb = new StringBuilder();
        sb.append("差异行 @").append(rowNo).append(": ");
        int len = Math.min(dbRow.length, fileRow.length) - 1;

        for (int i = 0; i < len; i++) {
            if (!isCellEqual(dbRow[i], fileRow[i], types[i])) {
                sb.append("[").append(names[i]).append("] ")
                        .append("CSV:'").append(fileRow[i]).append("' != ")
                        .append("DB:'").append(dbRow[i]).append("'; ");
            }
        }
        return sb.toString();
    }


    // 辅助：获取数组最后一个元素作为行号
    private Long getRowNo(Object row) {
        // 如果某一边为 null，行号设为 MAX_VALUE 以便进入后续的判断分支
        if (row == null) return null;
        if (row instanceof Object[]) {
            Object[] arr = (Object[]) row;
            return Long.parseLong(String.valueOf(arr[arr.length - 1]));
        }
        return null; // Should not happen for DB
    }

    private Long getRowNo(String[] row) {
        if (row == null) return null;
        return Long.parseLong(row[row.length - 1]);
    }

    // 有时间时在将下面的按类型比对合并进来
    private boolean isCellEqual_x1(Object dbVal, String fileStr, int sqlType) {
        // 1. 处理 NULL
        if (dbVal == null) {
            return fileStr == null || fileStr.trim().isEmpty();
        }
        if (fileStr == null || fileStr.trim().isEmpty()) {
            return false; // DB有值，文件没值
        }
        String csvVal = fileStr.trim();

        switch (sqlType) {
            // === 数值比较 (最关键) ===
            case Types.DECIMAL:
            case Types.NUMERIC:
                BigDecimal dbDec = (BigDecimal) dbVal;
                try {
                    BigDecimal fileDec = new BigDecimal(csvVal);
                    return dbDec.compareTo(fileDec) == 0;
                } catch (NumberFormatException e) {
                    return false;
                }

            case Types.INTEGER:
            case Types.BIGINT:
                // 数据库可能是 Integer 或 Long，统一转 String 或 Long 比较
                try {
                    long dbLong = ((Number) dbVal).longValue();
                    long fileLong = Long.parseLong(csvVal);
                    return dbLong == fileLong;
                } catch (Exception e) {
                    return false;
                }

                // === 时间比较 ===
            case Types.TIMESTAMP:
                Timestamp dbTime = (Timestamp) dbVal;
                // 复用之前的 parseTimestamp 逻辑把 csvVal 转成 Timestamp
                Timestamp fileTime = parseTimestamp(csvVal);
                return dbTime.compareTo(fileTime) == 0;

            // === 字符串 ===
            default:
                return String.valueOf(dbVal).trim().equals(csvVal);
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