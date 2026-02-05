package com.example.moveprog.config;

import com.example.moveprog.enums.VerifyStrategy;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriterSettings;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * 应用配置
 */
@Data
@Component
@ConfigurationProperties(prefix = "app") // 对应 application.yml 中的 app:
public class AppProperties {

    /**
     * 【新增】当前节点对外服务的 IP 地址
     * 必须配置为局域网内其他机器可访问的 IP (如万兆网卡 IP)
     */
    private String currentNodeIp;

    // 1. CSV 相关配置 (对应 app.csv)
    private Job job = new Job();

    // 嵌套配置类
    private Csv csv = new Csv();

    // 2. 性能/并发相关配置 (对应 app.performance)
    private Performance performance = new Performance();

    private Transcode transcode = new Transcode();

    private String jdbcOptions = "?Unicode=true&characterEncoding=utf8&allowLoadLocalInfile=true&useCompression=true";

    private LoadJdbc loadJdbc = new LoadJdbc();

    private Verify verify = new Verify();

    // 线程池配置嵌套对象
    private ExecutorGroup executor = new ExecutorGroup();

    // 嵌套配置对象
    private TargetDbConfig targetDbConfig = new TargetDbConfig();

    @Data
    public static class ExecutorGroup {
        private ExecutorConfig transcode = new ExecutorConfig();
        private ExecutorConfig load = new ExecutorConfig();
        private ExecutorConfig verify = new ExecutorConfig();
    }

    @Data
    public static class ExecutorConfig {
        private int coreSize = 10;
        private int maxSize = 10;
        private int queueCapacity = 100;
    }

    @Data
    public static class TargetDbConfig {
        private int maxPoolSize = 60;      // 默认值
        private int minIdle = 10;
        private long connectionTimeout = 30000;
        private boolean autoCommit = false;
    }

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
        // 1. 基础手动配置 (作为自动探测失败时的兜底默认值)
        // ==========================================

        // 如果 yaml 没配，就是 ','；配了就是 yaml 的值
        /**
         * 换行符配置
         * 可选值: "\n", "\r\n"
         */
        private String lineSeparator = "\n";

        private char delimiter = ',';

        private char quote = '"';

        private char quoteEscape = '"';

        private char comment = '#';

        // ==========================================
        // 2. [新增] 自动探测增强配置 (Auto-Detection)
        // ==========================================
        private boolean detectLineSeparator = false;
        // 是否开启分隔符自动探测 (默认 true，省心)
        private boolean detectDelimiter = false;
        // 是否开启引号自动探测
        private boolean detectQuote = false;

        // 探测时采样的行数 (读前1000行来猜格式，防止前几行数据异常导致猜错)
        private int detectSampleRows = 1000;

        // ==========================================
        // 3. 解析健壮性配置
        // ==========================================

        // 是否包含表头
        private boolean headerExtraction = false;

        // 单个单元格最大字符数 (防止缓冲区溢出)
        // 默认 4096 太小，建议 ETL 场景默认给大一点，比如 20000
        private int maxCharsPerColumn = 50000;

        // [关键] 跳过文件头部的行数 (比如大机文件第一行有时是系统生成的垃圾信息)
        private long skipRows = 0;

        // 是否忽略值前面的空格 (例如 " abc" -> "abc")
        private boolean ignoreLeadingWhitespaces = true;

        // 是否忽略值后面的空格 (例如 "abc " -> "abc")
        private boolean ignoreTrailingWhitespaces = true;

        // 读取时将什么字符串视为 null (比如 CSV 里写的是 NULL 字样)
        private String nullValue = ""; // 将 CSV 中的 "null" 或空串转为空字符串
        private boolean tunneling = false; // 大机用转义来表示编码不支持的生僻字（大机IBM1388可能有这种情况)

        /**
         * 工厂方法：生成读取配置
         */
        public CsvParserSettings toParserSettings() {
            CsvParserSettings settings = new CsvParserSettings();

            // --- 1. 基础格式 (作为默认值)
            CsvFormat format = settings.getFormat();
            format.setDelimiter(this.delimiter);
            format.setQuote(this.quote);
            format.setQuoteEscape(this.quoteEscape);
            format.setComment(this.comment);

            // --- A. 物理层：确定“行”的边界 ---
            if (this.detectLineSeparator) {
                // 1. 如果开启了探测，让框架自己去猜
                settings.setLineSeparatorDetectionEnabled(true);
            } else {
                // 2. 如果关闭了探测，则必须手动指定一个明确的换行符
                settings.setLineSeparatorDetectionEnabled(false); // 显式关闭探测
                settings.getFormat().setLineSeparator(this.lineSeparator); // 使用配置的兜底值
            }

            // 3. [增强] 自动探测逻辑
            // --- B. 逻辑层：确定“列”的结构 ---
            // 这两个开关启动的是“格式分析器”，它在“确定了行”之后才开始工作
            settings.setDelimiterDetectionEnabled(this.detectDelimiter);
            settings.setQuoteDetectionEnabled(this.detectQuote);

            // 设置采样行数：在大文件场景下，多读一点样本能避免误判
            if (this.detectDelimiter || this.detectQuote || this.detectLineSeparator) {
                settings.setFormatDetectorRowSampleCount(this.detectSampleRows);
            }

            // 4. 跳过行数 (注意：这里要用 skipRows，不要用 maxCharsPerColumn)
            if (this.skipRows > 0) {
                settings.setNumberOfRowsToSkip(this.skipRows);
            }

            // 5. 其他健壮性配置
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
            settings.setQuoteAllFields(true); // TDSQL 建议全引, 通常数据库导入建议全引用，或者加一个配置项控制
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
        /**
         * 关单时自动清除刷出文件
         */
        private boolean autoCleanOnClose = true;
    }

    /**
     * 转码配置
     */
    @Data
    public static class Transcode {
        /**
         * 最大转码错误行，超过则转码状态变成失败
         */
        @Min(value = 1, message = "最大转码失败行 max-error-count 不能小于 1")
        private int maxErrorCount = 1000;
    }


    /**
     * 目标端装载时, 预先执行sql
     * 用于准备环境(比如禁用约束，日志等）
     */
    @Data
    public static class LoadJdbc {
        private List<String> preSqlList = Arrays.asList(
                "SET unique_checks=0",
                "SET foreign_key_checks=0",
                "SET sql_log_bin=0"
        );

        /**
         * 【新增】JDBC Batch 大小
         */
        private int batchSize = 5000;

        private int maxRetries = 3;
        private int queryTimeout = 600; // 10分钟超时

        /**
         * 【新增】是否使用 LOAD DATA LOCAL INFILE
         * true: 使用极速模式 (默认)
         * false: 使用通用 JDBC Batch Insert 模式 (保底, 慢)
         */
        private boolean useLocalInfile = false;

        private String tableQuoteChar = "`";
        private String columnQuoteChar = "`";
        // 列名
        private String columnNameCsvId = "csv_id";
        private String columnNameSourceRowNo = "source_row_no";
    }

    /**
     * 验证配置
     */
    @Data
    public static class Verify {
        @NotNull(message = "校验策略 strategy 不能为空")
        private VerifyStrategy strategy = VerifyStrategy.USE_SOURCE_FILE;

        @Min(value = 1, message = "最大差异数 max-diff-count 不能小于 1")
        private int maxDiffCount = 1000;

        /**
         * 验证通过后删除中间输出(拆分文件等)文件
         */
        private boolean deleteSplitVerifyPass = false;

        private int fetchSize = -1;
    }

}