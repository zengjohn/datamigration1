package com.example.moveprog.dto;

import java.util.List;

public class OkFileContent {
    /**
     * ddl （定义表的列)
     */
    public String ddl;

    public String schema;

    public String table;

    /**
     * csv文件列表(表数据)
     */
    public List<String> csv;
}