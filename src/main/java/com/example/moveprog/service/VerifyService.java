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
import com.example.moveprog.util.FastEscapeHandler;
import com.example.moveprog.util.MigrationOutputDirectorUtil;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 校验服务
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class VerifyService {
    private final StateManager stateManager;

    private final MigrationJobRepository jobRepo;
    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;

    private final CoreComparator coreComparator; // 注入上一轮写的比对器
    private final TargetDatabaseConnectionManager targetDatabaseConnectionManager;
    private final MigrationArtifactManager migrationArtifactManager;

    // 注入 AppProperties 用于获取配置...
    private final JdbcHelper jdbcHelper;
    private final AppProperties config;

    public void execute(Long splitId) {
        CsvSplit csvSplit = splitRepo.findById(splitId).orElse(null);
        if (null == csvSplit || csvSplit.getStatus() != CsvSplitStatus.VERIFYING) {
            return;
        }

        migrationArtifactManager.cleanVerifyArtifacts(csvSplit);
        MigrationJob migrationJob = jobRepo.findById(csvSplit.getJobId()).orElseThrow();

        // 2. 准备迭代器
        try (VerifyDiffWriter diffWriter = createVerifyDiffWriter(migrationJob, csvSplit);
             JdbcRowIterator dbIter = createDbIterator(csvSplit);
             CloseableRowIterator<String> fileIter = createFileIterator(csvSplit)) {

            log.info("开始校验切片: {}", splitId);

            // 3. 【核心调用】执行比对，并获取差异数
            long diffCount = coreComparator.compareStreams(csvSplit.getJobId(), fileIter, dbIter, diffWriter);

            // 4. 【核心判断】根据差异数决定最终状态
            if (diffCount == 0) {
                log.info("校验通过: 切片 ID={}", splitId);
                migrationArtifactManager.deleteEmptyVerifyResultFile(csvSplit);
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

            // 查询detail状态，如果是完成， 则根据配置清除文件
            if (config.getVerify().isDeleteSplitVerifyPass()) {
                qianyiDetail = detailRepo.findById(csvSplit.getDetailId()).orElseThrow();
                if (DetailStatus.FINISHED.equals(qianyiDetail.getStatus())) {
                    migrationArtifactManager.cleanQianyiDetailArtifacts(qianyiDetail);
                }
            }

        } catch (Exception e) {
            log.error("校验过程发生系统异常: {}", e.getMessage(), e);
            stateManager.switchSplitStatus(splitId, CsvSplitStatus.FAIL_VERIFY, "系统异常: " + e.getMessage());
            // 失败也要刷新父级
            stateManager.refreshDetailStatus(csvSplit.getDetailId());
            // 失败时不删除文件！方便运维人员去磁盘上查看这个文件到底哪里有问题
        }
    }

    private VerifyDiffWriter createVerifyDiffWriter(MigrationJob migrationJob, CsvSplit split) throws IOException {
        return new VerifyDiffWriter(migrationJob, split.getQianyiId(), split.getId(), config.getVerify().getMaxDiffCount());
    }

    private JdbcRowIterator createDbIterator(CsvSplit split) throws Exception {
        // SQL: 强制按 source_row_no 排序，保证流式读取顺序与文件一致
        String sql = jdbcHelper.verifySelectSql(split.getId());
        JdbcRowIterator dbIter = new JdbcRowIterator(targetDatabaseConnectionManager, split.getJobId(), sql, config.getVerify().getFetchSize());
        return dbIter;
    }

    // 工厂方法：根据策略创建不同的文件迭代器
    private CloseableRowIterator createFileIterator(CsvSplit split) throws Exception {

        Pair<String,Boolean> actualSplitPath = MigrationOutputDirectorUtil.getActualSplitPath(split);
        // 1. 优先检查是否存在补丁文件 (UTF-8)
        if (actualSplitPath.getValue().booleanValue()) {
            log.info("Split[{}] 发现补丁文件，将使用补丁文件作为比对源", split.getId());
            AppProperties.CsvDetailConfig utf8Split = config.getCsv().getUtf8Split();
            CsvParserSettings settings = utf8Split.toParserSettings();
            CsvParser csvParser = new CsvParser(settings);
            // 读取拆分文件 (UTF-8)：
            return new CsvRowIterator(actualSplitPath.getKey(), true, csvParser,
                    CharsetFactory.resolveCharset(utf8Split.getEncoding()), split.getStartRowNo());
        }

        VerifyStrategy strategy = config.getVerify().getStrategy();
        if (strategy == VerifyStrategy.USE_SOURCE_FILE) {
            // 策略 B: 读源文件 IBM1388 + Skip
            QianyiDetail detailById = detailRepo.findById(split.getDetailId()).orElse(null);
            String sourcePath = detailById.getSourceCsvPath();

            AppProperties.CsvDetailConfig ibmSource = config.getCsv().getIbmSource();
            CsvParserSettings settings = ibmSource.toParserSettings();
            // 配置跳过
            if (split.getStartRowNo() >=1) {
                settings.setNumberOfRowsToSkip(split.getStartRowNo()-1);
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

}