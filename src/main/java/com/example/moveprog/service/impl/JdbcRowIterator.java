package com.example.moveprog.service.impl;

import com.example.moveprog.service.IJdbcRowIterator;
import com.example.moveprog.service.TargetDatabaseConnectionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.sql.*;
import java.util.List;
import java.util.Objects;

@Slf4j
public class JdbcRowIterator implements IJdbcRowIterator<Object> {

    private final String sql; // 方便调试

    private String[] columnNames;
    private int[] columnTypes;

    private Connection conn;
    private PreparedStatement ps;
    private ResultSet rs;

    private int colCount;
    private boolean hasNext;

    public JdbcRowIterator(TargetDatabaseConnectionManager targetDatabaseConnectionManager, Long jobId, String sql, List<Object> params, int fetchSize) throws SQLException {
        this.sql = sql;
        boolean success = false; // 标记是否构造成功

        try {
            this.conn = targetDatabaseConnectionManager.getConnection(jobId, true);
            this.ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            this.ps.setQueryTimeout(600);
            // 设置参数
            if (params != null) {
                for (int i = 0; i < params.size(); i++) {
                    this.ps.setObject(i + 1, params.get(i));
                }
            }
            if (fetchSize < 0) {
                // 【关键】必须设置为 Integer.MIN_VALUE 才能开启 MySQL 的流式读取
                this.ps.setFetchSize(Integer.MIN_VALUE); // 开启流式
            } else {
                this.ps.setFetchSize(fetchSize); // url中需要配置 useCursorFetch=true
            }


            if (log.isDebugEnabled()) {
                log.debug("{} splitId: {}", sql);
            }

            this.rs = ps.executeQuery();
            Assert.isTrue(Objects.nonNull(this.rs), "executeQuery(" + sql + ")返回的rs不能为空");
            ResultSetMetaData meta = this.rs.getMetaData();
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

            success = true; // 构造成功
        } finally {
            // 如果构造失败，必须关闭已申请的资源
            if (!success) {
                close();
            }
        }
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

    @Override
    public String[] getColumnNames() {
        return columnNames;
    }

    @Override
    public int[] getColumnTypes() {
        return columnTypes;
    }

}