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
            sql.append("TABLE_SCHEMA='").append(schema).append("'").append(" AND ") ;
        }
        sql.append("TABLE_NAME='").append(table).append("'") ;
        return sql.toString();
    }

    public String qianyiTableName(Long qianyiId) {
        Qianyi qianyi = qianyiRepo.findById(qianyiId).orElseThrow();
        return tableNameQuote(qianyi.getSchemaName(), qianyi.getTableName());
    }

    public String columnQuote(String columnName) {
        return config.getLoadJdbc().getColumnQuoteChar() + columnName + config.getLoadJdbc().getColumnQuoteChar();
    }

    public String tableNameQuote(String schema, String table) {
        if (Objects.isNull(schema) || schema.isEmpty()) {
            return quoteSchemaOrTable(table);
        }
        return quoteSchemaOrTable(schema) + "." + quoteSchemaOrTable(table);
    }

    private String quoteSchemaOrTable(String name) {
        return config.getLoadJdbc().getTableQuoteChar() + name + config.getLoadJdbc().getTableQuoteChar();
    }

    /**
     * 产生删除(目标表)装载数据sql
     * @param splitId
     * @return
     */
    public String deleteSql(Long splitId) {
        CsvSplit csvSplit = csvSplitRepository.findById(splitId).orElseThrow();
        Qianyi qianyi = qianyiRepo.findById(csvSplit.getQianyiId()).orElseThrow();
        String tableName = qianyiTableName(qianyi.getId());

        String sql = "DELETE FROM " + tableName + " WHERE " + columnQuote(config.getLoadJdbc().getColumnNameCsvId()) + "=" + splitId;
        return sql;
    }

    public String verifySelectSql(Long splitId) throws IOException {
        CsvSplit csvSplit = csvSplitRepository.findById(splitId).orElseThrow();

        Qianyi qianyi = qianyiRepo.findById(csvSplit.getQianyiId()).orElseThrow();
        String tableName = qianyiTableName(qianyi.getId());

        String ddlFilePath = qianyi.getDdlFilePath();
        List<String> columnNames = new ArrayList<>(SchemaParseUtil.parseColumnNamesFromDdl(ddlFilePath));
        columnNames.add(config.getLoadJdbc().getColumnNameSourceRowNo());
        String columnList = columnNames.stream().map(c -> columnQuote(c)).collect(Collectors.joining(","));

        // SQL: 强制按 source_row_no 排序，保证流式读取顺序与文件一致
        String sql = "SELECT " + columnList + " FROM " + tableName +
                " WHERE " + columnQuote(config.getLoadJdbc().getColumnNameCsvId()) + " = " + csvSplit.getId() +
                " ORDER BY " + columnQuote(config.getLoadJdbc().getColumnNameSourceRowNo()) + " ASC";
        return sql;
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

        String splitCsvPath = MigrationOutputDirectorUtil.getActualSplitPath(csvSplit);

        String safePath = splitCsvPath.replace("\\", "/");

        AppProperties.CsvDetailConfig utf8Split = config.getCsv().getUtf8Split();

        StringBuilder sb = new StringBuilder();
        sb.append("LOAD DATA LOCAL INFILE '").append(safePath).append("' ");
        sb.append("INTO TABLE ").append(queroTableName).append(" ");
        sb.append("CHARACTER SET utf8mb4 ");
        sb.append("FIELDS TERMINATED BY '").append(utf8Split.getDelimiter()).append("' ");
        sb.append("OPTIONALLY ENCLOSED BY '").append(utf8Split.getQuote()).append("' ");
        sb.append("LINES TERMINATED BY '").append(utf8Split.getLineSeparator()).append("' ");

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
