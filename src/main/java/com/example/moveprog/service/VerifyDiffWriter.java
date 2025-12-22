package com.example.moveprog.service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class VerifyDiffWriter implements AutoCloseable {
    private final BufferedWriter writer;
    private int diffCount = 0;
    private final int maxDiff;

    public VerifyDiffWriter(String filePath, int maxDiff) throws IOException {
        this.writer = new BufferedWriter(new FileWriter(filePath));
        this.maxDiff = maxDiff;
    }

    /**
     * 记录差异
     * @throws DiffLimitExceededException 当差异超过限额时抛出
     */
    public void writeDiff(String content) throws IOException, DiffLimitExceededException {
        writer.write(content);
        writer.newLine();
        diffCount++;
        
        if (diffCount >= maxDiff) {
            throw new DiffLimitExceededException("差异数量超过阈值: " + maxDiff + "，停止比对");
        }
    }

    public int getDiffCount() {
        return diffCount;
    }

    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }
    
    // 自定义异常
    public static class DiffLimitExceededException extends Exception {
        public DiffLimitExceededException(String message) { super(message); }
    }
}