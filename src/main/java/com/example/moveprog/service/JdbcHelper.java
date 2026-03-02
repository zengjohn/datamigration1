package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.QianyiRepository;
import com.example.moveprog.util.MigrationOutputDirectorUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class JdbcHelper {

    private final QianyiRepository qianyiRepo;
    private final CsvSplitRepository csvSplitRepository;

    private final AppProperties config;

    public String checkExistsTableSql(String schema, String table) {
        StringBuilder sql = new StringBuilder();
        sql.append("select count(*) from information_schema.tables where ");
        if (schema != null && !schema.isEmpty()) {
            // 【安全修复】使用 escapeStringLiteral 转义
            sql.append("TABLE_SCHEMA='").append(escapeStringLiteral(schema)).append("'").append(" AND ") ;
        }
        sql.append("TABLE_NAME='").append(escapeStringLiteral(table)).append("'") ;
        return sql.toString();
    }

    public String qianyiTableName(Long qianyiId) {
        Qianyi qianyi = qianyiRepo.findById(qianyiId).orElseThrow();
        return tableNameQuote(qianyi.getTargetSchema(), qianyi.getTargetTableName());
    }

    public String columnQuote(String columnName) {
        // 【安全修复】使用 escapeIdentifier
        //return config.getLoadJdbc().getColumnQuoteChar() + columnName + config.getLoadJdbc().getColumnQuoteChar();
        return escapeIdentifier(columnName, config.getLoadJdbc().getColumnQuoteChar());
    }

    public String tableNameQuote(String schema, String table) {
        if (Objects.isNull(schema) || schema.isEmpty()) {
            return quoteSchemaOrTable(table);
        }
        return quoteSchemaOrTable(schema) + "." + quoteSchemaOrTable(table);
    }

    private String quoteSchemaOrTable(String name) {
        return escapeIdentifier(name, config.getLoadJdbc().getTableQuoteChar());
    }

    /**
     * 【新增】防御性标识符转义：将标识符包裹在引号中，并转义内部的引号
     * 例如：quoteChar=` name=user`table -> `user``table`
     */
    private String escapeIdentifier(String identifier, String quoteChar) {
        if (identifier == null) return "";
        if (quoteChar == null || quoteChar.isEmpty()) return identifier; // 无引号模式

        // 核心逻辑：将 identifier 中的 quoteChar 替换为 双份
        return quoteChar + identifier.replace(quoteChar, quoteChar + quoteChar) + quoteChar;
    }

    /**
     * 【新增】防御性字符串字面量转义：防止 ' OR '1'='1 注入
     * 适用于 SQL 中 'value' 的场景
     */
    private String escapeStringLiteral(String input) {
        if (input == null) return "";
        // 标准 SQL 转义：将 ' 替换为 ''
        // 同时转义反斜杠 (MySQL默认行为)
        return input.replace("\\", "\\\\").replace("'", "''");
    }

    /**
     * 产生删除(目标表)装载数据sql
     * @param splitId
     * @return Pair<SQL, 参数列表>
     */
    public Pair<String, List<Object>> deleteSql(Long splitId) {
        CsvSplit csvSplit = csvSplitRepository.findById(splitId).orElseThrow();
        Qianyi qianyi = qianyiRepo.findById(csvSplit.getQianyiId()).orElseThrow();
        String tableName = qianyiTableName(qianyi.getId());

        String sql = "DELETE FROM " + tableName + " WHERE " + columnQuote(config.getLoadJdbc().getColumnNameCsvId()) + "=?";
        return Pair.of(sql, Collections.singletonList(splitId));
    }

    public Pair<String, List<Object>> verifySelectSql(Long splitId) throws IOException {
        CsvSplit csvSplit = csvSplitRepository.findById(splitId).orElseThrow();

        Qianyi qianyi = qianyiRepo.findById(csvSplit.getQianyiId()).orElseThrow();
        String tableName = qianyiTableName(qianyi.getId());

        String ddlFilePath = qianyi.getDdlFilePath();
        List<String> columnNames = new ArrayList<>(SchemaParseUtil.parseColumnNamesFromDdl(ddlFilePath));
        columnNames.add(config.getLoadJdbc().getColumnNameSourceRowNo());
        String columnList = columnNames.stream().map(c -> columnQuote(c)).collect(Collectors.joining(","));

        // SQL: 强制按 source_row_no 排序，保证流式读取顺序与文件一致
        // 使用占位符 ?
        String sql = "SELECT " + columnList + " FROM " + tableName +
                " WHERE " + columnQuote(config.getLoadJdbc().getColumnNameCsvId()) + " = ?" +
                " ORDER BY " + columnQuote(config.getLoadJdbc().getColumnNameSourceRowNo()) + " ASC";

        return Pair.of(sql, Collections.singletonList(csvSplit.getId()));
    }

    /**
     * 构建 Insert SQL: INSERT INTO table (col1, col2, csv_id, source_row_no) VALUES (?, ?, ?)
     * @return
     */
    public Pair<String,List<String>> loadJdbcSql(Long splitId) throws IOException {

        CsvSplit csvSplit = csvSplitRepository.findById(splitId).orElseThrow();

        Qianyi qianyi = qianyiRepo.findById(csvSplit.getQianyiId()).orElseThrow();
        String queroTableName = qianyiTableName(qianyi.getId());

        String ddlFilePath = qianyi.getDdlFilePath();
        List<String> columnNameList = new ArrayList<>(SchemaParseUtil.parseColumnNamesFromDdl(ddlFilePath));
        columnNameList.add(config.getLoadJdbc().getColumnNameCsvId());
        columnNameList.add(config.getLoadJdbc().getColumnNameSourceRowNo());

        StringBuilder sql = new StringBuilder("INSERT INTO ").append(queroTableName).append(" (");
        sql.append(columnNameList.stream().map(c -> columnQuote(c)).collect(Collectors.joining(","))).append(") VALUES (");
        sql.append(columnNameList.stream().map(c -> "?").collect(Collectors.joining(",")));
        sql.append(")");

        return Pair.of(sql.toString(), columnNameList);
    }


    public String loadDataInfileSql(Long splitId) throws IOException {

        CsvSplit csvSplit = csvSplitRepository.findById(splitId).orElseThrow();

        Qianyi qianyi = qianyiRepo.findById(csvSplit.getQianyiId()).orElseThrow();
        String queroTableName = qianyiTableName(qianyi.getId());

        String ddlFilePath = qianyi.getDdlFilePath();

        Pair<String, Boolean> actualSplitPath = MigrationOutputDirectorUtil.getActualSplitPath(csvSplit);

        String safePath = actualSplitPath.getKey().replace("\\", "/");
        // 【安全修复】对路径进行转义
        safePath = escapeStringLiteral(safePath);

        AppProperties.CsvDetailConfig utf8Split = config.getCsv().getUtf8Split();

        StringBuilder sb = new StringBuilder();
        sb.append("LOAD DATA LOCAL INFILE '").append(safePath).append("' ");
        sb.append("INTO TABLE ").append(queroTableName).append(" ");
        sb.append("CHARACTER SET utf8mb4 ");

        // 【安全修复】转义 CSV 格式选项
        sb.append("FIELDS TERMINATED BY '").append(escapeStringLiteral(String.valueOf(utf8Split.getDelimiter()))).append("' ");
        sb.append("OPTIONALLY ENCLOSED BY '").append(escapeStringLiteral(String.valueOf(utf8Split.getQuote()))).append("' ");
        sb.append("LINES TERMINATED BY '").append(escapeStringLiteral(utf8Split.getLineSeparator())).append("' ");

        // --- 关键部分：列映射 ---
        // CSV 文件的结构是: [DDL列1, DDL列2, ... DDL列N, 行号]
        // SQL 语法: (col1, col2, ..., @var_lineno)

        sb.append("(");
        List<String> columnNames = new ArrayList<>(SchemaParseUtil.parseColumnNamesFromDdl(ddlFilePath));
        // 1. 拼接DDL中的业务列名
        for (String col : columnNames) {
            sb.append(columnQuote(col)).append(", ");
        }
        sb.append(columnQuote(config.getLoadJdbc().getColumnNameSourceRowNo()));
        sb.append(")");
        // -- 【关键】在这里把当前的 csvid 赋值给每一行
        sb.append(" SET ").append(columnQuote(config.getLoadJdbc().getColumnNameCsvId())).append("=").append(splitId);

        return sb.toString();
    }


}
