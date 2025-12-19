package com.example.moveprog.service;

import java.util.Iterator;

/**
 * 通用行数据迭代器
 * 作用：抹平 JDBC ResultSet 和 CSV Parser 的差异，
 * 让比对逻辑只关心 "String[]" 数据。
 */
public interface CloseableRowIterator extends Iterator<String[]>, AutoCloseable {
    // 继承了 hasNext(), next() 和 close()
    // 返回的 String[] 代表一行数据
}