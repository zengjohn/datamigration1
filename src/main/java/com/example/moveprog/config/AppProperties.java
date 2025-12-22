package com.example.moveprog.config;

import com.univocity.parsers.csv.CsvFormat;
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

        // ==========================================
        // 1. CsvFormat 基础属性 (赋默认值)
        // ==========================================

        // 如果 yaml 没配，就是 ','；配了就是 yaml 的值
        private char delimiter = ',';

        private char quote = '"';

        private char quoteEscape = '"';

        private char comment = '#';

        /**
         * 换行符配置
         * 可选值: "\n", "\r\n", 或者 "AUTO" (自动检测)
         * 默认值: "AUTO" (推荐用于处理来源不确定的文件)
         */
        private String lineSeparator = "AUTO";

        // ==========================================
        // 2. ParserSettings 调优属性 (赋默认值)
        // ==========================================

        // 是否包含表头
        private boolean headerExtraction = false;

        // 单个单元格最大字符数 (防止缓冲区溢出)
        // 默认 4096 太小，建议 ETL 场景默认给大一点，比如 20000
        private int maxCharsPerColumn = 20000;

        // 是否忽略值前面的空格 (例如 " abc" -> "abc")
        private boolean ignoreLeadingWhitespaces = true;

        // 是否忽略值后面的空格 (例如 "abc " -> "abc")
        private boolean ignoreTrailingWhitespaces = true;

        // 读取时将什么字符串视为 null (比如 CSV 里写的是 NULL 字样)
        private String nullValue = null;
        private boolean tunneling = false; // 大机用转义来表示编码不支持的生僻字（大机IBM1388可能有这种情况)

        /**
         * 工厂方法：生成读取配置
         */
        public CsvParserSettings toParserSettings() {
            CsvParserSettings settings = new CsvParserSettings();

            // --- 1. 设置 Format ---
            CsvFormat format = settings.getFormat();
            format.setDelimiter(this.delimiter);
            format.setQuote(this.quote);
            format.setComment(this.comment);
            // --- 2. 设置换行符逻辑 ---
            if ("AUTO".equalsIgnoreCase(this.lineSeparator) || this.lineSeparator == null) {
                // 开启自动检测 (Univocity 会读取文件前几个字节来猜)
                settings.setLineSeparatorDetectionEnabled(true);
            } else {
                // 强制指定 (注意处理转义字符)
                format.setLineSeparator(this.lineSeparator);
            }
            // --- 3. 设置调优参数 ---
            settings.setHeaderExtractionEnabled(this.headerExtraction);
            settings.setMaxCharsPerColumn(this.maxCharsPerColumn);
            settings.setIgnoreLeadingWhitespaces(this.ignoreLeadingWhitespaces);
            settings.setIgnoreTrailingWhitespaces(this.ignoreTrailingWhitespaces);
            if (this.nullValue != null) {
                settings.setNullValue(this.nullValue);
            }
            settings.setSkipEmptyLines(true); // 是否跳过空行
            // 默认不开启多线程读取 (ETL 场景下通常由上层控制并发)
            settings.setReadInputOnSeparateThread(false);

            return settings;
        }

        /**
         * 生成写入配置 (WriterSettings)
         */
        public CsvWriterSettings toWriterSettings() {
            CsvWriterSettings settings = new CsvWriterSettings();
            CsvFormat format = settings.getFormat();
            format.setDelimiter(this.delimiter);
            format.setQuote(this.quote);
            format.setQuoteEscape(this.quoteEscape);
            // 写出时通常明确指定换行符，不要用 AUTO
            String separator = "AUTO".equalsIgnoreCase(this.lineSeparator) ? "\n" : this.lineSeparator;
            format.setLineSeparator(separator);
            settings.setSkipEmptyLines(true);
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
        private int maxDiffCount = 1000;
        private String verifyResultBasePath = "d:/data/verify_results/";
    }

}