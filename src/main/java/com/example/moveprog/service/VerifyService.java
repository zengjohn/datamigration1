package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.*;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.VerifyStrategy;
import com.example.moveprog.repository.*;
import com.example.moveprog.service.impl.CsvRowIterator;
import com.example.moveprog.service.impl.JdbcRowIterator;
import com.example.moveprog.util.CharsetFactory;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 校验服务
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class VerifyService {
    private final StateManager stateManager;

    private final MigrationJobRepository jobRepo;
    private final QianyiRepository qianyiRepo;
    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;

    private final CoreComparator coreComparator; // 注入上一轮写的比对器

    // 注入 AppProperties 用于获取配置...
    private final AppProperties config;

    public void execute(Long splitId) {
        // 1. 抢占任务
        if (!stateManager.switchSplitStatus(splitId, CsvSplitStatus.VERIFYING, null)) return;

        try {
            VerifyStrategy strategy = VerifyStrategy.valueOf(config.getVerify().getStrategy());
            log.info("    [Verify] 开始校验 Split: {}", splitId);

            // 2. 准备基础数据
            CsvSplit csvSplit = splitRepo.findById(splitId).orElseThrow();
            Qianyi qianyi = qianyiRepo.findById(csvSplit.getQianyiId()).orElseThrow();
            MigrationJob migrationJob = jobRepo.findById(csvSplit.getJobId()).orElseThrow();

            doVerifySingleSplit(csvSplit, qianyi.getDdlFilePath(), qianyi.getTableName(), migrationJob, strategy);

            // 2. 成功提交 -> PASS
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.PASS, "校验一致");

            // 3. 【关键】清理临时文件 (代码略)

            // 4. 【关键】刷新父级 Detail 状态
            QianyiDetail qianyiDetail = detailRepo.findById(csvSplit.getDetailId()).orElseThrow();
            stateManager.refreshDetailStatus(qianyiDetail.getId()); // 需在Manager加个简单查询方法，或这里先查再传

        } catch (Exception e) {
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.FAIL_VERIFY, e.getMessage());
            // 失败也要刷新父级
            // stateManager.refreshDetailStatus(...)
        }
    }

    /**
     * 单个分片的具体执行逻辑
     * 捕获所有异常，确保不中断主线程的 join
     */
    private void doVerifySingleSplit(CsvSplit split, String ddlFilePath, String tableName, MigrationJob job, VerifyStrategy strategy) {
        try {
            // 设置状态为处理中 (可选，便于UI展示进度)
            // split.setStatus(CsvSplitStatus.VERIFYING);
            // splitRepo.save(split);
            // 结果文件路径: /path/split_100_diff.txt
            String diffFilePath =  config.getVerify().getVerifyResultBasePath() + "split_" + split.getId() + "_diff.txt";

            String dbUrl = job.getTargetJdbcUrl();
            String user = job.getTargetUser();
            String pwd = job.getTargetPassword();

            List<String> columnNames = SchemaParseUtil.parseColumnNamesFromDdl(ddlFilePath);
            String columnList = columnNames.stream().collect(Collectors.joining(",")) + ","+"source_row_no";
            // SQL: 强制按 source_row_no 排序，保证流式读取顺序与文件一致
            String sql = "SELECT " + columnList + " FROM " + tableName + " WHERE csvid = " + split.getId() + " ORDER BY source_row_no";

            // 使用 try-with-resources 自动关闭两个迭代器
            try (
                    // 1. 构建 DB 迭代器
                    CloseableRowIterator dbIter = new JdbcRowIterator(dbUrl, user, pwd, sql);

                    // 2. 构建 文件 迭代器 (工厂方法见上一轮回答)
                    CloseableRowIterator fileIter = createFileIterator(split, strategy);

                    // 【新增】创建差异写入器
                    VerifyDiffWriter diffWriter = new VerifyDiffWriter(diffFilePath, config.getVerify().getMaxDiffCount())
            ) {
                // 3. 核心比对
                coreComparator.compareStreams((JdbcRowIterator)dbIter, fileIter, diffWriter);

                // 检查比对结果
                if (diffWriter.getDiffCount() > 0) {
                    split.setStatus(CsvSplitStatus.FAIL_VERIFY);
                    split.setErrorMsg("验证失败，差异数: " + diffWriter.getDiffCount() + "，详见: " + diffFilePath);
                } else {
                    split.setStatus(CsvSplitStatus.PASS);
                    split.setErrorMsg("验证通过");
                }
            }

            // 成功
            split.setStatus(CsvSplitStatus.PASS);
            split.setErrorMsg(null);
            splitRepo.save(split);

        } catch (Exception e) {
            log.error("分片比对失败: detail id: {} split id: {}", split.getDetailId(), split.getId(), e);
            split.setStatus(CsvSplitStatus.FAIL_VERIFY);
            // 截取部分错误信息防止数据库字段溢出
            String msg = e.getMessage();
            split.setErrorMsg(msg != null && msg.length() > 500 ? msg.substring(0, 500) : msg);
            splitRepo.save(split);
            // 注意：这里吞掉异常，不要抛出，否则 CompletableFuture.join 会报错中断其他任务
        }
    }

    // 工厂方法：根据策略创建不同的文件迭代器
    private CloseableRowIterator createFileIterator(CsvSplit split, VerifyStrategy strategy) throws Exception {
        if (strategy == VerifyStrategy.USE_SOURCE_FILE) {
            // 策略 B: 读源文件 IBM1388 + Skip
            QianyiDetail detailById = detailRepo.findById(split.getDetailId()).orElse(null);
            String sourcePath = detailById.getSourceCsvPath();

            AppProperties.CsvDetailConfig ibmSource = config.getCsv().getIbmSource();
            CsvParserSettings settings = ibmSource.toParserSettings();
            // 配置跳过
            if (split.getStartRowNo() > 1) {
                settings.setNumberOfRowsToSkip(split.getStartRowNo());
            }
            settings.setNumberOfRecordsToRead(split.getRowCount());
            CsvParser csvParser = new CsvParser(settings);
            // 配置读取限制
            // 读取源文件：偏移量通常是 0 (因为 context.currentLine() 就是真实行号)
            return new CsvRowIterator(sourcePath, false, csvParser,
                    CharsetFactory.resolveCharset(ibmSource.getEncoding()), 0);
        } else {
            AppProperties.CsvDetailConfig utf8Split = config.getCsv().getUtf8Split();
            CsvParserSettings settings = utf8Split.toParserSettings();
            CsvParser csvParser = new CsvParser(settings);
            // 读取拆分文件 (UTF-8)：
            return new CsvRowIterator(split.getSplitFilePath(), true, csvParser,
                    CharsetFactory.resolveCharset(utf8Split.getEncoding()), split.getStartRowNo());
        }
    }

}