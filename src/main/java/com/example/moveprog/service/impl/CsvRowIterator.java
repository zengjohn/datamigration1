package com.example.moveprog.service.impl;

import com.example.moveprog.service.CloseableRowIterator;
import com.univocity.parsers.csv.CsvParser;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;

@Slf4j
public class CsvRowIterator implements CloseableRowIterator {
    private final InputStreamReader reader;
    private final CsvParser parser;
    private String[] nextRow;
    private final String filePath;
    private final boolean splitCsvFile; // 拆分的csv文件(utf8编码) true， 原始的IBM1388 CSV文件 false
    // 如果是拆分文件，可能需要加上偏移量（比如第2个切片从10001行开始）
    // 如果是读源文件，这个 offset是 0
    private final long rowNumberOffset;

    /**
     * @param filePath 文件路径
     * @param charset 编码 (UTF-8 或 IBM1388)
     */
    public CsvRowIterator(String filePath, boolean splitCsvFile, CsvParser parser, Charset charset, long rowNumberOffset) throws Exception {
        this.filePath = filePath;
        this.splitCsvFile = splitCsvFile;
        this.rowNumberOffset = rowNumberOffset;
        if (log.isDebugEnabled()) {
            log.debug("filePath: {}, rowNumberOffset: {}", filePath, rowNumberOffset);
        }
        this.reader = new InputStreamReader(new FileInputStream(filePath), charset);
        this.parser = parser;
        this.parser.beginParsing(reader);
        this.nextRow = parser.parseNext(); // 预读
    }

    @Override
    public boolean hasNext() {
        return nextRow != null;
    }

    private String[] convertRow(String[] rowData, long currentLineIndex) {
        if (null == rowData) {
            return null;
        }

        if (splitCsvFile) {
            return rowData;
        }

        // 加上偏移量 (如果是切分文件，物理行号是1，但逻辑行号可能是10001)
        long logicRowNo = currentLineIndex + rowNumberOffset;

        // 3. 将行号追加到数组的最后一位
        // 扩容数组：原长度 + 1
        String[] rowWithLineNo = Arrays.copyOf(rowData, rowData.length + 1);
        // 将行号转为 String 放入最后
        rowWithLineNo[rowData.length] = String.valueOf(logicRowNo);

        return rowWithLineNo;
    }

    @Override
    public String[] next() {
        // 2. 【关键】获取当前行在文件中的物理行号
        // 注意：即便 skipEmptyLines=true，Univocity 也能返回正确的物理行号
        long currentLineIndex = parser.getContext().currentLine();
        String[] current = nextRow;

        nextRow = parser.parseNext(); // 预读下一行
        return convertRow(current, currentLineIndex);
    }

    @Override
    public void close() {
        try { if (reader != null) reader.close(); } catch (Exception e) {}
    }
}