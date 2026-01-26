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
        // 使用简单的 BufferedReader 读取，因为格式很简单
        try (BufferedReader br = Files.newBufferedReader(Paths.get(ddlPath), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // 按逗号分割，取第一列
                // 注意：如果列名本身包含逗号，这里需要更复杂的解析，但通常数据库列名不会有逗号
                String[] parts = line.split(",");
                if (parts.length >= 1) {
                    columns.add(parts[0].trim());
                }
            }
        }
        return columns;
    }


}
