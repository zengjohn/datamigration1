package com.example.moveprog.config;

import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriterSettings;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "app") // 对应 application.yml 中的 app:
public class AppProperties {

    // 1. CSV 相关配置 (对应 app.csv)
    // 嵌套配置类
    private Csv csv = new Csv();

    // 2. 性能/并发相关配置 (对应 app.performance)
    private Performance performance = new Performance();

    private Job job = new Job();

    private Verify verify = new Verify();

    @Data
    public static class Csv {
        // 场景 A: IBM 源文件配置
        private CsvDetailConfig ibmSource = new CsvDetailConfig();

        // 场景 B: UTF-8 拆分文件配置
        private CsvDetailConfig utf8Split = new CsvDetailConfig();
    }

    /**
     * 复用具体的 CSV 细节配置类
     * 包含 encoding, delimiter 以及生成 Settings 的工厂方法
     */
    @Data
    public static class CsvDetailConfig {
        private String encoding = "UTF-8";
        private String lineSeparator = "\n";
        private char delimiter = ',';
        private char quote = '"';
        private char quoteEscape = '"';
        private boolean headerExtraction = false;
        private int maxCharsPerColumn = 4096;
        private boolean tunneling = false; // 大机用转义来表示编码不支持的生僻字（大机IBM1388可能有这种情况)

        /**
         * 生成读取配置 (ParserSettings)
         */
        public CsvParserSettings toParserSettings() {
            CsvParserSettings settings = new CsvParserSettings();
            settings.getFormat().setLineSeparator(this.lineSeparator);
            settings.getFormat().setDelimiter(this.delimiter);
            settings.getFormat().setQuote(this.quote);
            settings.getFormat().setQuoteEscape(this.quoteEscape);
            settings.setHeaderExtractionEnabled(this.headerExtraction);
            settings.setMaxCharsPerColumn(this.maxCharsPerColumn);
            settings.setReadInputOnSeparateThread(false);
            return settings;
        }

        /**
         * 生成写入配置 (WriterSettings)
         */
        public CsvWriterSettings toWriterSettings() {
            CsvWriterSettings settings = new CsvWriterSettings();
            settings.getFormat().setLineSeparator(this.lineSeparator);
            settings.getFormat().setDelimiter(this.delimiter);
            settings.getFormat().setQuote(this.quote);
            settings.getFormat().setQuoteEscape(this.quoteEscape);
            settings.setQuoteAllFields(true);
            return settings;
        }
    }

    @Data
    public static class Performance {
        // 缓冲区大小 (字节)，默认 8MB
        private int readBufferSize = 8 * 1024 * 1024;
        // 写入缓冲区 (字节)，默认 1MB
        private int writeBufferSize = 1 * 1024 * 1024;
        // 切分行数
        private int splitRows = 500_000;
    }

    @Data
    public static class Job {
        private String outputDir;
        private String errorDir;
    }

    @Data
    public static class Verify {
        private String strategy = "USE_SOURCE_FILE";
    }

}