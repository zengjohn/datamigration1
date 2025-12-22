package com.example.moveprog.service.impl;

import com.example.moveprog.service.CloseableRowIterator;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;

@Slf4j
public class JdbcRowIterator implements CloseableRowIterator {

    private final String[] columnNames;
    private final int[] columnTypes;

    private final Connection conn;
    private final String sql; // 方便调试
    private final Statement ps;
    private final ResultSet rs;
    private final int colCount;
    private boolean hasNext;

    public JdbcRowIterator(String url, String user, String pwd, String sql) throws SQLException {
        this.sql = sql;

        this.conn = java.sql.DriverManager.getConnection(url, user, pwd);
        this.ps = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.ps.setFetchSize(Integer.MIN_VALUE); // 开启流式
        if (log.isDebugEnabled()) {
            log.debug("{} splitId: {}", sql);
        }

        this.rs = ps.executeQuery(sql);
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        this.columnTypes = new int[colCount];
        this.columnNames = new String[colCount];
        for (int i = 0; i < colCount; i++) {
            // JDBC index starts from 1
            this.columnTypes[i] = meta.getColumnType(i + 1);
            this.columnNames[i] = meta.getColumnLabel(i + 1);
        }

        this.colCount = rs.getMetaData().getColumnCount();
        this.hasNext = rs.next(); // 预读第一行
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public Object[] next() {
        try {
            Object[] row = new Object[colCount];
            // JDBC 下标从1开始
            for (int i = 0; i < colCount; i++) {
                row[i] = rs.getObject(i + 1);
            }
            // 移动游标状态
            hasNext = rs.next();
            return row;
        } catch (SQLException e) {
            throw new RuntimeException("数据库读取异常", e);
        }
    }

    @Override
    public void close() {
        // 依次关闭资源
        try { if (rs != null) rs.close(); } catch (Exception e) {}
        try { if (ps != null) ps.close(); } catch (Exception e) {}
        try { if (conn != null) conn.close(); } catch (Exception e) {}
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public int[] getColumnTypes() {
        return columnTypes;
    }

}