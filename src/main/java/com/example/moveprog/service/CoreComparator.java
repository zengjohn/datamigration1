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

@Slf4j
@Component
public class CoreComparator {
    @Autowired
    private JobControlManager jobControlManager;

    /**
     * 核心比对方法 (纯逻辑，不涉及 IO 细节)
     * @param dbIter 数据库数据的迭代器
     * @param fileIter 文件数据的迭代器 (无论是 UTF8 还是 IBM1388)
     */
    public void compareStreams(Long jobId, JdbcRowIterator dbIter, CloseableRowIterator<String> fileIter, VerifyDiffWriter diffWriter) throws IOException, SQLException {
        // 1. 获取元数据 (用于打印列名)
        int[] colTypes = dbIter.getColumnTypes();
        String[] colNames = dbIter.getColumnNames();

        // 2. 初始化缓存行
        Object[] dbRow = dbIter.hasNext() ? dbIter.next() : null;
        String[] fileRow = fileIter.hasNext() ? fileIter.next() : null;

        try {
            int count = 0;
            while (dbRow != null || fileRow != null) {
                // 【埋点】每处理 1000 行检查一次
                // 没必要每行都查，太浪费性能；也没必要查太少，响应太慢
                if (count++ % 1000 == 0) {
                    jobControlManager.checkJobState(jobId);
                }

                // 获取行号 (假设行号在数组最后一位)
                // 注意：需要处理 Long 类型转换
                Long dbRowNo = getRowNo(dbRow);
                Long fileRowNo = getRowNo(fileRow);

                if (dbRowNo == null && fileRowNo == null) break; // 双双结束

                // === Case 1: 源端存在，目标端不存在 (File 有, DB 没有) ===
                // 此时 fileRowNo < dbRowNo (或者 DB 已经读完了)
                if (dbRowNo == null || (fileRowNo != null && fileRowNo < dbRowNo)) {
                    // 源端 !{行号}, 目标端null
                    String msg = String.format("源端 !{%d}, 目标端 null", fileRowNo);
                    diffWriter.writeDiff(msg);

                    // File 指针后移，DB 不动
                    fileRow = fileIter.hasNext() ? fileIter.next() : null;
                }

                // === Case 2: 源端不存在，目标端存在 (DB 有, File 没有) ===
                // 此时 dbRowNo < fileRowNo (或者 File 已经读完了)
                else if (fileRowNo == null || (dbRowNo != null && dbRowNo < fileRowNo)) {
                    // 源端 null, 目标端 !{行号}
                    String msg = String.format("源端 null, 目标端 !{%d}", dbRowNo);
                    diffWriter.writeDiff(msg);

                    // DB 指针后移，File 不动
                    dbRow = dbIter.hasNext() ? dbIter.next() : null;
                }

                // === Case 3: 两端都存在，比较内容 ===
                else {
                    // 行号相等，开始比对字段
                    String diffContent = checkContentDiff(dbRow, fileRow, colTypes, colNames, dbRowNo);
                    if (diffContent != null) {
                        diffWriter.writeDiff(diffContent);
                    }

                    // 两个指针都后移
                    dbRow = dbIter.hasNext() ? dbIter.next() : null;
                    fileRow = fileIter.hasNext() ? fileIter.next() : null;
                }
            }
        } catch (JobStoppedException e) {
            // 【特殊处理】用户叫停
            log.warn("任务被中断: {}", e.getMessage());
            // 此时应该把状态重置回 NEW，或者保持 TRANSCODING 等待下次“启动修复”
            // 建议：不做处理，直接 return。因为下次启动时的 StartupTaskResetter 会负责把 TRANSCODING 重置为 NEW
            return;
        } catch (VerifyDiffWriter.DiffLimitExceededException e) {
            log.warn("比对差异过多，提前终止: {}", e.getMessage());
            // 这里不抛出异常，视为正常截断，方便 Service 层处理后续状态
        }
    }

    /**
     * 比对具体内容
     * 返回 null 表示无差异，返回字符串表示差异信息
     */
    private String checkContentDiff(Object[] dbRow, String[] fileRow, int[] colTypes, String[] colNames, Long rowNo) {
        // 减1是因为最后一列是行号，不参与业务数据比对
        int compareLen = Math.min(dbRow.length, fileRow.length) - 1;

        StringBuilder srcSb = new StringBuilder();
        StringBuilder tgtSb = new StringBuilder();
        boolean hasDiff = false;

        for (int i = 0; i < compareLen; i++) {
            Object dbVal = dbRow[i];
            String fileVal = fileRow[i];
            int type = colTypes[i];

            // 格式化输出 (用于拼接结果)
            String colName = colNames[i];
            String dbValStr = String.valueOf(dbVal);
            String fileValStr = (fileVal == null) ? "null" : fileVal;

            if (!isCellEqual(dbVal, fileVal, type)) {
                hasDiff = true;
                // 发现差异，拼接详细信息
                // 格式: user_name: 源值(csv), 目标值(db) ???
                // 用户要求: src: user_id: val, balance: val ... tgt: ...
                // 这里我们先把所有字段拼起来，最后再组装
            }

            // 无论是否有差异，都按用户要求的格式拼装数据以便查看
            if (i > 0) {
                srcSb.append(", ");
                tgtSb.append(", ");
            }
            srcSb.append(colName).append(": ").append(fileValStr);
            tgtSb.append(colName).append(": ").append(dbValStr);
        }

        if (hasDiff) {
            // 源端 @{行号} src: ..., tgt: ...
            return String.format("源端 @{%d} src: %s, tgt: %s", rowNo, srcSb.toString(), tgtSb.toString());
        }
        return null;
    }

    // 辅助：获取数组最后一个元素作为行号
    private Long getRowNo(Object row) {
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
    private boolean isCellEqual(Object dbVal, String fileStr, int sqlType) {
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