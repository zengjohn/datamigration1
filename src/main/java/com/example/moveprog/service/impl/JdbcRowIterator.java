package com.example.moveprog.service.impl;

import com.example.moveprog.service.CloseableRowIterator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcRowIterator implements CloseableRowIterator {

    private final Connection conn;
    private final PreparedStatement ps;
    private final ResultSet rs;
    private final int colCount;
    private boolean hasNext;

    public JdbcRowIterator(String url, String user, String pwd, String sql, Long splitId) throws SQLException {
        this.conn = java.sql.DriverManager.getConnection(url, user, pwd);
        this.ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.ps.setFetchSize(Integer.MIN_VALUE); // 开启流式
        this.ps.setLong(1, splitId);
        this.rs = ps.executeQuery();
        this.colCount = rs.getMetaData().getColumnCount();
        this.hasNext = rs.next(); // 预读第一行
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public String[] next() {
        try {
            String[] row = new String[colCount];
            // JDBC 下标从1开始
            for (int i = 0; i < colCount; i++) {
                row[i] = rs.getString(i + 1);
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

}