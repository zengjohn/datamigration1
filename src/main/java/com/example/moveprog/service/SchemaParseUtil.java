package com.example.moveprog.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SchemaParseUtil {

    /**
     * 解析 DDL CSV 文件获取列名
     * 格式: 列名, 类型
     */
    public static List<String> parseColumnNamesFromDdl(String ddlPath) throws IOException {
        List<String> columns = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(ddlPath), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#") || line.startsWith("--")) continue; // 跳过注释

                // 优化：同时支持逗号分隔和空格分隔
                // 1. 尝试按逗号分割 (CSV格式)
                String[] parts = line.split(",");

                // 2. 如果没有逗号，尝试按空格/Tab分割 (SQL格式: COL_NAME VARCHAR(10))
                if (parts.length == 1) {
                    parts = line.split("\\s+");
                }

                if (parts.length >= 1) {
                    // 去除可能存在的引号或反引号
                    String colName = parts[0].trim().replaceAll("^[\"`']+|[\"`']$", "");
                    if (!colName.isEmpty()) {
                        columns.add(colName);
                    }
                }
            }
        }
        return columns;
    }


}
