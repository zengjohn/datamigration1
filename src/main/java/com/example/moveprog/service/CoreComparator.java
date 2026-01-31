package com.example.moveprog.service;

import com.example.moveprog.exception.JobStoppedException;
import com.example.moveprog.service.impl.JdbcRowIterator;
import com.example.moveprog.util.DBUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

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
        int[] colSqlTypes = dbIter.getColumnTypes();
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

                if (dbRowNo == null && fileRowNo == null) break;

                // === Case 1: 源端存在，目标端不存在 (File 有, DB 没有) ===
                // 此时 fileRowNo < dbRowNo (或者 DB 已经读完了)
                if (dbRowNo == null || (fileRowNo != null && fileRowNo < dbRowNo)) {
                    // 源端 !{行号}, 目标端null
                    String msg = String.format("CSV !{%d}, DB: null", fileRowNo);
                    diffWriter.writeDiff(msg);

                    // File 指针后移，DB 不动
                    fileRow = fileIter.hasNext() ? fileIter.next() : null;
                }

                // === Case 2: 源端不存在，目标端存在 (DB 有, File 没有) ===
                // 此时 dbRowNo < fileRowNo (或者 File 已经读完了)
                else if (fileRowNo == null || (dbRowNo != null && dbRowNo < fileRowNo)) {
                    // 源端 null, 目标端 !{行号}
                    String msg = String.format("CSV: null, DB: !{%d}", dbRowNo);
                    diffWriter.writeDiff(msg);

                    // DB 指针后移，File 不动
                    dbRow = dbIter.hasNext() ? dbIter.next() : null;
                }

                // === Case 3: 两端都存在，比较内容 ===
                else {
                    // 3. 行号一致，比对内容
                    // 核心修复：isRowEqual 返回 false 时才去拼接字符串，极大提升性能
                    if (!isRowEqual(dbRow, fileRow, colSqlTypes)) {
                        String diffMsg = formatDiffDetail(dbRow, fileRow, colSqlTypes, colNames, dbRowNo);
                        diffWriter.writeDiff(diffMsg);
                    }

                    // 4. 两个指针都后移
                    dbRow = dbIter.hasNext() ? dbIter.next() : null;
                    fileRow = fileIter.hasNext() ? fileIter.next() : null;
                }
            }

            return diffWriter.getDiffCount().get();
        } catch (JobStoppedException e) {
            // 【特殊处理】用户叫停
            log.warn("任务被中断: {}", e.getMessage());
            // 此时应该把状态重置回 NEW，或者保持 TRANSCODING 等待下次“启动修复”
            // 建议：不做处理，直接 return。因为下次启动时的 StartupTaskResetter 会负责把 TRANSCODING 重置为 NEW
            return -1;
        } catch (VerifyDiffWriter.DiffLimitExceededException e) {
            log.warn("差异过多截断: {}", e.getMessage());
            return -1;
        }

    }

    /**
     * 快速比对行（不生成垃圾对象）
     */
    private static boolean isRowEqual(Object[] dbRow, String[] fileRow, int[] colSqlTypes) {
        // 注意：dbRow 和 fileRow 长度可能包含 row_no，比对时要排除最后一列
        int len = Math.min(dbRow.length, fileRow.length) - 1;

        for (int i = 0; i < len; i++) {
            Object dbVal = dbRow[i];
            String fileVal = fileRow[i];
            int type = colSqlTypes[i];
            if (!isCellEqual(dbVal, fileVal, type)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isCellEqual(Object dbVal, String fileStr, int sqlType) {
        // 1. 判空逻辑
        boolean dbNull = (dbVal == null);
        boolean fileNull = (fileStr == null || fileStr.trim().isEmpty() || "null".equalsIgnoreCase(fileStr.trim()));
        if (dbNull && fileNull) return true;
        if (dbNull || fileNull) return false; // 一个有一个没有

        String csvVal = fileStr.trim();

        if (DBUtils.isNumber(sqlType) || DBUtils.isReal(sqlType)) {
            try {
                // 使用 BigDecimal 比较，忽略精度差异 (1.0 == 1.00)
                return new BigDecimal(dbVal.toString()).compareTo(new BigDecimal(csvVal)) == 0;
            } catch (Exception e) {
                return false;
            }
        }

        if (DBUtils.isTimeType(sqlType)) {
            return compareAsTime(dbVal, csvVal);
        }

        if (DBUtils.isDateType(sqlType) ||
                DBUtils.isTimestamp(sqlType)) {
            return compareAsTimestamp(dbVal, csvVal);
        }

        return String.valueOf(dbVal).trim().equals(csvVal);
    }

    private static boolean compareAsTime(Object dbVal, String csvVal) {
        try {
            Time dbTime = (Time)dbVal;
            Time csvTime = parseCsvTime(csvVal);
            return dbTime.compareTo(csvTime) == 0;
        } catch (Exception e) {
            if (log.isTraceEnabled()) {
                log.trace("解析时间报错, 用字符串比较, dbVal: {}, csvVal: {}", dbVal, csvVal, e);
            }
            return String.valueOf(dbVal).equals(csvVal);
        }
    }

    /**
     * 将两边都转为 Timestamp 进行比对
     */
    private static boolean compareAsTimestamp(Object dbVal, String csvVal) {
        try {
            Timestamp t1 = toTimestamp(dbVal);
            Timestamp t2 = parseCsvTimestamp(csvVal);

            if (t1 == null || t2 == null) return false;

            // 比较 (t1.compareTo(t2) == 0 表示相等)
            // 如果不需要毫秒级精确，可以 t1.getTime()/1000 == t2.getTime()/1000
            int ret = t1.compareTo(t2);
            return ret == 0;
        } catch (Exception e) {
            // 如果解析挂了，回退到字符串比较，或者直接认为不相等
            // log.warn("时间比对异常: DB={} vs CSV={}", dbVal, csvVal);
            return String.valueOf(dbVal).equals(csvVal);
        }
    }

    /**
     * 将 DB 对象转为 Timestamp
     */
    private static Timestamp toTimestamp(Object val) {
        if (val instanceof java.sql.Timestamp) return (Timestamp) val;
        if (val instanceof java.time.LocalDateTime) return Timestamp.valueOf((LocalDateTime) val);
        if (val instanceof java.sql.Date) return new Timestamp(((java.sql.Date) val).getTime());
        if (val instanceof java.util.Date) return new Timestamp(((java.util.Date) val).getTime());
        // 尝试转换字符串
        return parseCsvTimestamp(val.toString());
    }

    /**
     * 解析 CSV 字符串为 Timestamp (支持多种格式)
     */
    private static Timestamp parseCsvTimestamp(String val) {
        // 1. 尝试 JDBC 标准格式
        try {
            return Timestamp.valueOf(val);
        } catch (Exception e) {
            // ignore
        }

        // 2. 尝试自定义格式器
        for (DateTimeFormatter fmt : DATE_PARSERS) {
            try {
                // 尝试解析为 LocalDateTime
                LocalDateTime ldt = LocalDateTime.parse(val, fmt);
                return Timestamp.valueOf(ldt);
            } catch (Exception e) {
                // 有些格式只有日期没有时间 (yyyy/M/d) -> 解析为 LocalDate 再转
                try {
                    java.time.LocalDate ld = java.time.LocalDate.parse(val, fmt);
                    return Timestamp.valueOf(ld.atStartOfDay());
                } catch (Exception ex) {
                    // continue
                }
            }
        }
        return null;
    }

    private static Time parseCsvTime(String val) {
        val = StringUtils.replaceChars(val, "/", ":");
        String[] splits = StringUtils.split(val, ":");

        if (splits.length == 1) {
            String hour = splits[0].trim();
            if (hour.length() == 1) hour = "0"+hour;
            return Time.valueOf(hour + ":00:00");
        }

        if (splits.length == 2) {
            String hour = splits[0].trim();
            if (hour.length() == 1) hour = "0"+hour;

            String min = splits[1].trim();
            if (hour.length() == 1) min = "0"+min;

            return Time.valueOf(hour + ":" + min + ":00");
        }

        return Time.valueOf(val);
    }

    // --- 辅助方法 ---
    private static String formatDiffDetail(Object[] dbRow, String[] fileRow, int[] types, String[] names, Long rowNo) {
        StringBuilder sb = new StringBuilder();
        sb.append("差异 @").append(rowNo).append(": ");
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

    private Long getRowNo(Object row) {
        if (row == null) return null;
        if (row instanceof Object[]) {
            Object[] arr = (Object[]) row;
            return Long.parseLong(String.valueOf(arr[arr.length - 1]));
        }
        return null;
    }

    private Long getRowNo(String[] row) {
        if (row == null) return null;
        return Long.parseLong(row[row.length - 1]);
    }

    // 定义支持的 CSV 时间格式 (包含您提到的斜杠格式)
    private static final DateTimeFormatter[] DATE_PARSERS = {
            // 兼容斜杠格式 (2024/1/15 14:30)
            // 使用 DateTimeFormatterBuilder 来构建灵活的解析器
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy/M/d")
                    .optionalStart().appendPattern(" H:m:s").optionalEnd()
                    .optionalStart().appendPattern(" H:m").optionalEnd()
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                    .toFormatter(Locale.ENGLISH),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
            // 标准格式
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd"),

            // 兼容点号或横杠的其他变体
            DateTimeFormatter.ofPattern("yyyy.MM.dd HH:mm:ss")
    };

}