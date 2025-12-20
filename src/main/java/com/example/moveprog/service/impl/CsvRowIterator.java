package com.example.moveprog.service.impl;

import com.example.moveprog.service.CloseableRowIterator;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class CsvRowIterator implements CloseableRowIterator {
    private final InputStreamReader reader;
    private final CsvParser parser;
    private String[] nextRow;

    /**
     * @param filePath 文件路径
     * @param charset 编码 (UTF-8 或 IBM1388)
     */
    public CsvRowIterator(String filePath, CsvParser parser, Charset charset) throws Exception {
        this.reader = new InputStreamReader(new FileInputStream(filePath), charset);

        this.parser = parser;
        //this.parser = new CsvParser(settings);
        this.parser.beginParsing(reader);
        this.nextRow = parser.parseNext(); // 预读
    }

    @Override
    public boolean hasNext() {
        return nextRow != null;
    }

    @Override
    public String[] next() {
        String[] current = nextRow;
        nextRow = parser.parseNext();
        return current;
    }

    @Override
    public void close() {
        try {
            if (reader != null) reader.close();
        } catch (Exception e) {
            // ignore
        }
    }
}