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
     * @param skipRows 需要跳过的行数 (UTF-8传0, 源文件传 startRow-1)
     * @param limitRows 需要读取的行数 (UTF-8传null/无限, 源文件传 rowCount)
     */
    public CsvRowIterator(String filePath, String charset, long skipRows, Long limitRows) throws Exception {
        this.reader = new InputStreamReader(new FileInputStream(filePath), Charset.forName(charset));
        
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        // 配置跳过
        if (skipRows > 0) {
            settings.setNumberOfRowsToSkip(skipRows);
        }
        // 配置读取限制
        if (limitRows != null && limitRows > 0) {
            //settings.setNumberOfRowsToProcess(limitRows);
        }

        this.parser = new CsvParser(settings);
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