package com.example.moveprog.service;

public interface IJdbcRowIterator<T>  extends CloseableRowIterator<T> {
    String[] getColumnNames();

    int[] getColumnTypes();
}
