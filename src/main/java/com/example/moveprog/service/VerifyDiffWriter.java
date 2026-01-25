package com.example.moveprog.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 差异记录器 - 修复版
 * 职责：记录差异到文件，并统计差异数量
 */
@Slf4j
public class VerifyDiffWriter implements AutoCloseable {
    private final String filePath;
    private final int maxDiff;
    @Getter
    private final AtomicLong diffCount = new AtomicLong(0);
    private BufferedWriter writer;

    public VerifyDiffWriter(String basePath, Long splitId, int maxDiff) throws IOException {
        Files.createDirectories(Paths.get(basePath));
        this.filePath = Paths.get(basePath, "split_" + splitId + "_diff.txt").toString();
        // 延迟创建文件：只有真正写入时才创建 writer，避免生成大量空文件
        this.maxDiff = maxDiff;
    }

    /**
     * 记录差异
     * @throws DiffLimitExceededException 当差异超过限额时抛出
     */
    public void writeDiff(String message) throws IOException, DiffLimitExceededException {
        try {
            if (writer == null) {
                writer = new BufferedWriter(new FileWriter(filePath));
            }

            writer.write(message);
            writer.newLine();

            // 【核心修复】每写一条，计数加一
            long lDiffCount = diffCount.incrementAndGet();

            if (lDiffCount >= maxDiff) {
                throw new DiffLimitExceededException("差异数量超过阈值: " + maxDiff + "，停止比对");
            }
        } catch (IOException e) {
            log.error("写入差异文件失败: {}", filePath, e);
        }
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                log.warn("关闭差异文件流失败", e);
            }
        }
    }
    
    // 自定义异常
    public static class DiffLimitExceededException extends Exception {
        public DiffLimitExceededException(String message) { super(message); }
    }

    public String getFilePath() {
        return filePath;
    }

}