package com.example.moveprog.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "app") // 对应 application.yml 中的 app:
public class AppProperties {

    // 嵌套配置类
    private Csv source = new Csv();
    private Csv output = new Csv();
    private Performance performance = new Performance();
    private Job job = new Job();

    @Data
    public static class Csv {
        private String encoding = "UTF-8";
        private char delimiter = ',';
        private char quote = '"';
        private char escape = '"';
        private String lineSeparator = "\n";
        private boolean headerPresent = false;
        private boolean tunneling = false; // 是否使用啦“转义序列打包（Tunneling）”技术是解决老旧大机字符集（IBM-1388/GBK）无法覆盖 Unicode 大字符集（CJK 扩展区）
    }

    @Data
    public static class Performance {
        // 缓冲区大小 (字节)，默认 8MB
        private int readBufferSize = 8 * 1024 * 1024;
        // 写入缓冲区 (字节)，默认 1MB
        private int writeBufferSize = 1 * 1024 * 1024;
        // 切分行数
        private int splitRows = 500_000;
        // Univocity 解析器内部 buffer (字符数)
        private int maxCharsPerColumn = 100_000;
    }

    @Data
    public static class Job {
        private String outputDir;
        private String errorDir;
    }

}