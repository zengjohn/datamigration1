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
            log.info("校验任务正在运行中，跳过: {}", detailId);
            return;
        }

        VerifyStrategy strategy = VerifyStrategy.valueOf(config.getVerify().getStrategy());

        try {
            QianyiDetail detail = detailRepo.findById(detailId).orElse(null);
            if (detail == null || detail.getStatus() != DetailStatus.WAIT_VERIFY) {
                return;
            }

            log.info("开始并发比对任务 DetailId={}, 策略={}", detailId, strategy);

            // 2. 准备基础数据
            Qianyi batch = qianyiRepo.findById(detail.getQianyiId()).orElseThrow();
            MigrationJob job = jobRepo.findById(batch.getJobId()).orElseThrow();

            // 3. 获取待比对的分片 (通常比对那些非 PASS 的，或者是全部重新比对，看业务需求)
            // 这里假设比对所有未通过的，或者 Failed 的允许重试
            List<CsvSplit> splits = splitRepo.findByDetailIdAndStatusNot(detailId, CsvSplitStatus.PASS);

            if (splits.isEmpty()) {
                log.info("没有需要比对的分片");
                markDetailSuccess(detail);
                return;
            }

            // 4. 并发执行 (Fan-out)
            List<CompletableFuture<Void>> futures = splits.stream()
                    .map(split -> CompletableFuture.runAsync(() -> {
                        // 每个分片单独执行比对
                        doVerifySingleSplit(split, batch.getTableName(), job, strategy);
                    }, verifyExecutor)) // <--- 使用专用线程池
                    .collect(Collectors.toList());

            // 5. 等待所有任务完成 (Join)
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // 6. 汇总结果 (Fan-in)
            // 再次查询数据库，看是否还有非 PASS 的状态
            long failCount = splitRepo.countByDetailIdAndStatusNot(detailId, CsvSplitStatus.PASS);

            if (failCount == 0) {
                markDetailSuccess(detail);
            } else {
                detail.setStatus(DetailStatus.WAIT_VERIFY); // 或 PARTIAL_FAIL
                detail.setErrorMsg(failCount + " 个分片比对失败，请查看子任务详情");
                detailRepo.save(detail);
            }

        } catch (Exception e) {
            log.error("比对主流程异常", e);
            QianyiDetail detail = detailRepo.findById(detailId).orElse(null);
            if (null != detail) {
                detail.setStatus(DetailStatus.WAIT_VERIFY);
                detail.setErrorMsg("系统异常: " + e.getMessage());
                detailRepo.save(detail);
            }
        } finally {
            // 2. 【出门解锁】
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
    private void doVerifySingleSplit(CsvSplit split, String tableName, MigrationJob job, VerifyStrategy strategy) {
        try {
            // 设置状态为处理中 (可选，便于UI展示进度)
            // split.setStatus(CsvSplitStatus.VERIFYING); splitRepo.save(split);

            String dbUrl = job.getTargetJdbcUrl();
            String user = job.getTargetUser();
            String pwd = job.getTargetPassword();

            // SQL: 强制按 source_row_no 排序，保证流式读取顺序与文件一致
            String sql = "SELECT * FROM " + tableName + " WHERE csvid = ? ORDER BY source_row_no";

            // 使用 try-with-resources 自动关闭两个迭代器
            try (
                    // 1. 构建 DB 迭代器
                    CloseableRowIterator dbIter = new JdbcRowIterator(dbUrl, user, pwd, sql, split.getId());

                    // 2. 构建 文件 迭代器 (工厂方法见上一轮回答)
                    CloseableRowIterator fileIter = createFileIterator(split, strategy)
            ) {
                // 3. 核心比对
                coreComparator.compareStreams(dbIter, fileIter);
            }

            // 成功
            split.setStatus(CsvSplitStatus.PASS);
            split.setErrorMsg(null);
            splitRepo.save(split);

        } catch (Exception e) {
            log.error("分片比对失败: SplitId={}", split.getId(), e);
            split.setStatus(CsvSplitStatus.FAIL);
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
            // 假设 detail 里能取到 sourceFilePath
            QianyiDetail detailById = detailRepo.findById(split.getDetailId()).orElse(null);
            String sourcePath = detailById.getSourceCsvPath();

            AppProperties.CsvDetailConfig ibmSource = config.getCsv().getIbmSource();
            CsvParserSettings settings = ibmSource.toParserSettings();
            // 配置跳过
            if (split.getStartRowNo() > 1) {
                settings.setNumberOfRowsToSkip(split.getStartRowNo() - 1);
            }
            CsvParser csvParser = new CsvParser(settings);
            // 配置读取限制
            settings.setNumberOfRecordsToRead(split.getRowCount());
            return new CsvRowIterator(sourcePath, csvParser, CharsetFactory.resolveCharset(ibmSource.getEncoding()));
        } else {
            AppProperties.CsvDetailConfig utf8Split = config.getCsv().getUtf8Split();
            CsvParserSettings settings = utf8Split.toParserSettings();
            CsvParser csvParser = new CsvParser(settings);
            // 策略 A: 读切分文件 UTF-8
            return new CsvRowIterator(split.getSplitFilePath(), csvParser, CharsetFactory.resolveCharset(utf8Split.getEncoding()));
        }
    }

}