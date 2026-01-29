package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.*;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.VerifyStrategy;
import com.example.moveprog.repository.*;
import com.example.moveprog.service.impl.CsvRowIterator;
import com.example.moveprog.service.impl.JdbcRowIterator;
import com.example.moveprog.util.CharsetFactory;
import com.example.moveprog.util.FastEscapeHandler;
import com.example.moveprog.util.MigrationOutputDirectorUtil;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
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
    private final TargetDatabaseConnectionManager targetDatabaseConnectionManager;

    // 注入 AppProperties 用于获取配置...
    private final AppProperties config;

    public void execute(Long splitId) {
        CsvSplit csvSplit = splitRepo.findById(splitId).orElse(null);
        if (null == csvSplit || csvSplit.getStatus() != CsvSplitStatus.VERIFYING) {
            return;
        }

        MigrationJob migrationJob = jobRepo.findById(csvSplit.getJobId()).orElseThrow();

        // 2. 准备迭代器
        try (VerifyDiffWriter diffWriter = createVerifyDiffWriter(migrationJob.getOutDirectory(), csvSplit);
             JdbcRowIterator dbIter = createDbIterator(csvSplit);
             CloseableRowIterator<String> fileIter = createFileIterator(csvSplit)) {

            log.info("开始校验切片: {}", splitId);

            // 3. 【核心调用】执行比对，并获取差异数
            long diffCount = coreComparator.compareStreams(csvSplit.getJobId(), fileIter, dbIter, diffWriter);

            // 4. 【核心判断】根据差异数决定最终状态
            if (diffCount == 0) {
                log.info("校验通过: 切片 ID={}", splitId);
                // 校验通过，删除可能的空差异文件（如果有的话）
                if (config.getVerify().isDeleteSplitVerifyPass()) {
                    deleteSplitFile(csvSplit);
                }
                deleteEmptyVerifyResultFile(migrationJob.getId(), splitId);
                stateManager.switchSplitStatus(splitId, CsvSplitStatus.PASS, "校验通过");
            } else {
                log.error("校验失败: 切片 ID={}，发现 {} 处差异", splitId, diffCount);
                String errorMsg = String.format("发现 %d 行不一致，详情见差异文件", diffCount);

                // 将状态置为 FAIL_VERIFY，并保存错误信息
                stateManager.switchSplitStatus(splitId, CsvSplitStatus.FAIL_VERIFY, errorMsg);
            }

            // 5. 【关键】刷新父级 Detail 状态
            QianyiDetail qianyiDetail = detailRepo.findById(csvSplit.getDetailId()).orElseThrow();
            stateManager.refreshDetailStatus(qianyiDetail.getId()); // 需在Manager加个简单查询方法，或这里先查再传

        } catch (Exception e) {
            log.error("校验过程发生系统异常: {}", e.getMessage(), e);
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.FAIL_VERIFY, "系统异常: " + e.getMessage());
            // 失败也要刷新父级
            stateManager.refreshDetailStatus(csvSplit.getDetailId());
            // 失败时不删除文件！方便运维人员去磁盘上查看这个文件到底哪里有问题
        }
    }

    private VerifyDiffWriter createVerifyDiffWriter(String outDirectory, CsvSplit split) throws IOException {
        String verifyResultBasePath = MigrationOutputDirectorUtil.verifyResultDirectory(outDirectory);
        return new VerifyDiffWriter(verifyResultBasePath, split.getId(), config.getVerify().getMaxDiffCount());
    }

    private JdbcRowIterator createDbIterator(CsvSplit split) throws Exception {
        Qianyi qianyi = qianyiRepo.findById(split.getQianyiId()).orElseThrow();

        String ddlFilePath = qianyi.getDdlFilePath();
        String tableName = qianyi.getTableName();

        List<String> columnNames = SchemaParseUtil.parseColumnNamesFromDdl(ddlFilePath);
        String columnList = columnNames.stream().collect(Collectors.joining(",")) + ","+"source_row_no";

        // SQL: 强制按 source_row_no 排序，保证流式读取顺序与文件一致
        String sql = "SELECT " + columnList + " FROM " + tableName + " WHERE csvid = " + split.getId() + " ORDER BY source_row_no";
        JdbcRowIterator dbIter = new JdbcRowIterator(targetDatabaseConnectionManager, split.getJobId(), sql, config.getVerify().getFetchSize());
        return dbIter;
    }

    // 工厂方法：根据策略创建不同的文件迭代器
    private CloseableRowIterator createFileIterator(CsvSplit split) throws Exception {
        VerifyStrategy strategy = config.getVerify().getStrategy();
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
            CsvRowIterator rawIter = new CsvRowIterator(sourcePath, false, csvParser,
                    CharsetFactory.resolveCharset(ibmSource.getEncoding()), 0);
            if (!ibmSource.isTunneling()) {
                return rawIter;
            }

            return new CloseableRowIterator<String>() {
                @Override
                public boolean hasNext() {
                    return rawIter.hasNext();
                }

                @Override
                public String[] next() {
                    String[] row = rawIter.next();
                    if (row == null) return null;

                    // 对每一列进行转义还原，与 TranscodeService 入库逻辑保持一致
                    String[] processedRow = new String[row.length];
                    for (int i = 0; i < row.length; i++) {
                        Pair<Boolean, String> unescape = FastEscapeHandler.unescape(row[i]);
                        processedRow[i] = unescape.getRight();
                    }
                    return processedRow;
                }

                @Override
                public void close() throws IOException {
                    rawIter.close();
                }
            };

        } else {
            AppProperties.CsvDetailConfig utf8Split = config.getCsv().getUtf8Split();
            CsvParserSettings settings = utf8Split.toParserSettings();
            CsvParser csvParser = new CsvParser(settings);
            // 读取拆分文件 (UTF-8)：
            return new CsvRowIterator(split.getSplitFilePath(), true, csvParser,
                    CharsetFactory.resolveCharset(utf8Split.getEncoding()), split.getStartRowNo());
        }
    }

    private void deleteSplitFile(CsvSplit split) {
        String splitFilePath = split.getSplitFilePath();
        File splitFile = new File(splitFilePath);
        if (splitFile.exists()) {
            splitFile.delete();
        }
    }

    private void deleteEmptyVerifyResultFile(Long jobId, Long splitId) {
        String diffFilePath = getDiffFilePath(jobId, splitId);
        File diffFile = new File(diffFilePath);
        if (diffFile.exists() && diffFile.length() == 0) {
            diffFile.delete();
        }
    }

    private String getDiffFilePath(Long jobId, Long splitId) {
        MigrationJob migrationJob = jobRepo.findById(jobId).orElseThrow();
        String verifyResultFile = MigrationOutputDirectorUtil.verifyResultFile(
                MigrationOutputDirectorUtil.verifyResultDirectory(migrationJob.getOutDirectory()),
                splitId);
        return verifyResultFile;
    }

}