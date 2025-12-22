package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.*;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.enums.VerifyStrategy;
import com.example.moveprog.repository.*;
import com.example.moveprog.service.impl.CsvRowIterator;
import com.example.moveprog.service.impl.JdbcRowIterator;
import com.example.moveprog.util.CharsetFactory;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class VerifyService {

    private final MigrationJobRepository jobRepo;
    private final QianyiRepository qianyiRepo;
    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;

    private final TaskLockManager lockManager;

    private final CoreComparator coreComparator; // 注入上一轮写的比对器
    // 注入刚才配置的专用线程池(使用 Verify 专用线程池)
    @Qualifier("verifyExecutor")
    private final Executor verifyExecutor;

    // 注入 AppProperties 用于获取配置...
    private final AppProperties config;

    /**
     * 执行比对的主入口
     * @param detailId 明细ID
     */
    @Async("taskExecutor") // 外层异步，避免阻塞 Controller
    public void execute(Long detailId) {
        // 1. 【进门加锁】
        if (!lockManager.tryLock(detailId)) {
            if (log.isDebugEnabled()) {
                log.debug("校验任务正在运行中，跳过: {}", detailId);
            }
            return;
        }

        VerifyStrategy strategy = VerifyStrategy.valueOf(config.getVerify().getStrategy());

        try {
            // 2. 状态双重检查
            QianyiDetail detail = detailRepo.findById(detailId).orElse(null);
            if (detail == null || detail.getStatus() != DetailStatus.WAIT_VERIFY) {
                return;
            }
            // 查询所以未成功的明细
            long notPassCount = splitRepo.countByDetailIdAndStatusNot(detailId, CsvSplitStatus.PASS);
            if (0 == notPassCount) {
                log.info("detail id: {} , 源文件: {} 没有需要比对的分片", detail.getId(), detail.getSourceCsvPath());
                markDetailSuccess(detail);
                return;
            }

            // 2. 准备基础数据
            Qianyi batch = qianyiRepo.findById(detail.getQianyiId()).orElseThrow();
            MigrationJob job = jobRepo.findById(batch.getJobId()).orElseThrow();

            // 3. 获取待比对的分片
            List<CsvSplit> splits = splitRepo.findByDetailIdAndStatus(detailId, CsvSplitStatus.WAIT_VERIFY);
            if (!splits.isEmpty()) {
                log.info("开始并发比对任务 DetailId={}, 策略={}", detailId, strategy);

                // 4. 并发执行 (Fan-out)
                List<CompletableFuture<Void>> futures = splits.stream()
                        .map(split -> CompletableFuture.runAsync(() -> {
                            // 每个分片单独执行比对
                            doVerifySingleSplit(batch.getDdlFilePath(), split, batch.getTableName(), job, strategy);
                        }, verifyExecutor)) // <--- 使用专用线程池
                        .collect(Collectors.toList());

                // 5. 等待所有任务完成 (Join)
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            }

            // 6. 汇总结果 (Fan-in)
            // 再次查询数据库，看是否还有非 PASS 的状态
            long failCount = splitRepo.countByDetailIdAndStatusNot(detailId, CsvSplitStatus.PASS);
            if (0 == failCount) {
                markDetailSuccess(detail);
            } else {
                detail.setStatus(DetailStatus.WAIT_VERIFY); // 或 PARTIAL_FAIL
                detail.setErrorMsg(failCount + " 个分片比对失败，请查看子任务详情");
                detailRepo.save(detail);
            }

        } catch (Exception e) {
            QianyiDetail detail = detailRepo.findById(detailId).orElse(null);
            if (null != detail) {
                log.error("比对主流程异常 detail id: {}, 源文件: {}", detail.getId(), detail.getSourceCsvPath(), e);
                detail.setStatus(DetailStatus.WAIT_VERIFY);
                detail.setErrorMsg("系统异常: " + e.getMessage());
                detailRepo.save(detail);
            } else {
                log.error("比对主流程异常", e);
            }
        } finally {
            // 7. 【出门解锁】
            lockManager.releaseLock(detailId);
        }
    }

    private void markDetailSuccess(QianyiDetail detail) {
        detail.setStatus(DetailStatus.PASS);
        detail.setErrorMsg("校验通过");
        detail.setUpdateTime(LocalDateTime.now());
        detailRepo.save(detail);
        log.info("DetailId={} 校验全部通过", detail.getId());
    }

    /**
     * 单个分片的具体执行逻辑
     * 捕获所有异常，确保不中断主线程的 join
     */
    private void doVerifySingleSplit(String ddlFilePath, CsvSplit split, String tableName, MigrationJob job, VerifyStrategy strategy) {
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