package com.example.moveprog.service;

import java.util.Iterator;

/**
 * 通用行数据迭代器
 */
public interface CloseableRowIterator<T> extends Iterator<T[]>, AutoCloseable {
    // 继承了 hasNext(), next() 和 close()
}