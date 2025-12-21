package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.example.moveprog.repository.QianyiRepository;
import com.example.moveprog.util.CharsetFactory;
import com.example.moveprog.util.FastEscapeHandler;
import com.google.gson.Gson;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TranscodeService {
    private final MigrationJobRepository migraRepo;
    private final QianyiRepository qianyiRepo;
    private final QianyiDetailRepository detailRepo;
    private final CsvSplitRepository splitRepo;

    private final TaskLockManager lockManager;
    
    // 注入 AppProperties 用于获取配置...
    private final AppProperties config;

    private final Gson gson = new Gson();

    @Async("taskExecutor") // 使用通用线程池, 关键：异步执行，否则调度器会卡死在这里等待文件转完
    public void execute(Long detailId) {
        // 1. 【进门加锁】
        if (!lockManager.tryLock(detailId)) {
            log.info("转码任务正在运行中，跳过: {}", detailId);
            return;
        }

        try {
            // 2. 【状态双重检查】(推荐)
            // 防止在排队等待线程池期间，状态已被修改
            QianyiDetail detail = detailRepo.findById(detailId).orElse(null);
            if (detail == null || detail.getStatus() != DetailStatus.NEW) {
                return;
            }
            log.info(">>> 开始转码, detail id: {}, 源文件: {}", detail.getId(), detail.getSourceCsvPath());

            // 3. 更新状态
            detail.setStatus(DetailStatus.TRANSCODING);
            detailRepo.save(detail);

            transcodeSingleSourceFile(detail.getQianyiId(), detail.getId(), detail.getSourceCsvPath());

            // 完成
            detail.setStatus(DetailStatus.WAIT_LOAD);
            detail.setProgress(100);
            detailRepo.save(detail);
            log.info(">>> 转码成功, detail id: {}, 源文件: {}", detail.getId(), detail.getSourceCsvPath());
        } catch (Exception e) {
            QianyiDetail detail = detailRepo.findById(detailId).orElse(null);
            if (null != detail) {
                log.error(">>> 转码失败, detail id: {}, 源文件: {}", detail.getId(), detail.getSourceCsvPath(), e);
                detail.setStatus(DetailStatus.FAIL_TRANSCODE);
                detail.setErrorMsg(e.getMessage());
                detailRepo.save(detail);
            } else {
                log.error("转码失败", e);
            }
        } finally {
            // 3. 【出门解锁】
            lockManager.releaseLock(detailId);
        }
    }


    private void transcodeSingleSourceFile(Long qianyiId, Long detailId, String sourcePath) throws IOException {
        // 1. 获取配置参数
        AppProperties.CsvDetailConfig ibmSource = config.getCsv().getIbmSource();
        AppProperties.Performance perfConfig = config.getPerformance();

        // 获取 IBM-1388 字符集对象，用于后续的验证
        Charset ibmCharset = CharsetFactory.resolveCharset(ibmSource.getEncoding());
        // 【关键配置】使用非严格模式，允许方案B捕获替换字符
        boolean strictMode = false;

        Qianyi findQianyiById = qianyiRepo.findById(qianyiId).orElse(null);
        List<String> ddlFilePath = SchemaParseUtil.parseColumnNamesFromDdl(findQianyiById.getDdlFilePath());
        int expectedColumns = ddlFilePath.size();

        // 【修改点 1】新增：创建 Encoder (用于新版验证) 和获取配置开关
        CharsetEncoder ibmEncoder = ibmCharset.newEncoder();
        boolean isTunneling = ibmSource.isTunneling();

        // 1. 创建 IBM-1388 严格模式 Reader
        try (Reader reader = new InputStreamReader(
                // 使用 buffered 包装，缓冲区设为 8MB
                new BufferedInputStream(Files.newInputStream(Paths.get(sourcePath)), perfConfig.getReadBufferSize()),
                // 创建严格模式的 Reader
                CharsetFactory.createDecoder(ibmCharset, strictMode));
        ) {
            // 根据大机实际情况配置，这里假设是标准CSV
            CsvParserSettings parserSettings = ibmSource.toParserSettings();

            CsvParser parser = new CsvParser(parserSettings);
            parser.beginParsing(reader);

            String[] originalLine;
            long lineNo = 1; // 大机行号
            long currentLineNoFromContext;

            // 初始化第一个 Writer
            int fileIndex = 1;
            CsvWriterContext csvWriterContext = null;
            CsvWriter errorWriter = null; // 新增：错误文件 Writer
            Path currentOutPath = null;
            long currentFileRows = 0;

            try {
                // 确保输出目录存在
                Files.createDirectories(Paths.get(config.getJob().getOutputDir()));
                Files.createDirectories(Paths.get(config.getJob().getErrorDir()));

                while (null != (originalLine = parser.parseNext())) {
                    currentLineNoFromContext = parser.getContext().currentLine();

                    // 1. 关键：检查主单是否停止 (优雅停机)
                    // 这里有严重的性能问题，在循环内部去查询数据库，考虑怎么样缓存!!!
                    boolean migrationJobActive = isMigrationJobActive(qianyiId);
                    if (!migrationJobActive) {
                        log.warn(">>> 迁移作业已停止，转码暂停: {}",detailId);
                        // 可以在这里保存断点行号
                        return; // 退出，finally块会释放锁
                    }

                    // 1. 准备要写入的数据对象 (默认就是原始数据)
                    String[] rowToWrite = originalLine;
                    // 如果配置啦tunneling
                    if (isTunneling) {
                        // 2. 解析生僻字 (用于入库)
                        // 解析出真实的汉字行 (例如把 "张\2CC56\三" 变成 "张𬱖三")
                        rowToWrite = new String[originalLine.length];
                        for(int i=0; i<originalLine.length; i++) {
                            rowToWrite[i] = FastEscapeHandler.unescape(originalLine[i]);
                        }
                    }

                    // --- 【方案B 植入点】开始 ---
                    String errorType = null;
                    List<ColumnErrorDetail> columnErrors = null;
                    if (expectedColumns > 0 && rowToWrite.length != expectedColumns) {
                        errorType = "COLUMN_COUNT_MISMATCH";
                    }
                    // --- 2. 列级检测：稳定性验证 ---
                    else {
                        columnErrors = validateRowStability(originalLine, rowToWrite, ibmCharset, ibmEncoder, isTunneling);
                        if (!columnErrors.isEmpty()) {
                            errorType = "STABILITY_CHECK_FAILED";
                        }
                    }

                    // --- 3. 发现错误，写入错误文件 ---
                    if (errorType != null) {
                        if (errorWriter == null) {
                            Path errorPath = Paths.get(config.getJob().getErrorDir(), detailId + "_error.csv");
                            errorWriter = createErrorWriter(errorPath);
                            // 表头：包含行级Base64 和 列级JSON
                            errorWriter.writeRow(new String[]{
                                    "LineNo", "ErrorType",
                                    "Row_Base64_IBM_Approx", "Row_Base64_UTF8",
                                    "Column_Details_JSON"
                            });
                        }

                        if ((1 == lineNo) && (null != errorType)) {
                            throwException(lineNo, errorType, originalLine, columnErrors, ibmCharset);
                        }

                        // 调用封装好的错误写入逻辑
                        writeErrorRecord(errorWriter, currentLineNoFromContext, errorType, originalLine, columnErrors, ibmCharset);

                        lineNo++;
                        continue; // 跳过正常写入
                    }
                    // --- 【方案B 植入点】结束 ---

                    // --- 以下是正常的成功处理逻辑 ---
                    // 懒加载创建文件
                    if (csvWriterContext == null) {
                        currentOutPath = Paths.get(config.getJob().getOutputDir(), detailId + "_" + fileIndex + ".csv");
                        csvWriterContext = createUtf8Writer(currentOutPath, currentLineNoFromContext);
                    }

                    // 2. 注入行号 (放到最后一列)
                    Object[] newRow = new Object[rowToWrite.length + 1];
                    for(int i=0; i<rowToWrite.length; i++) {
                        newRow[i] = rowToWrite[i];
                    }
                    newRow[rowToWrite.length] = currentLineNoFromContext; // ia-ibm1388-lineno

                    csvWriterContext.csvWriter.writeRow(newRow);
                    currentFileRows++;
                    lineNo++;

                    // 进度更新 (每 5000 行更新一次，避免频繁 IO)
                    if (lineNo % 5000 == 0) {
                        // 这里无法精确计算百分比，因为不知道总行数，可以先设个假进度或根据文件字节估算
                        // detail.setProgress(...);
                        // detailRepo.save(detail);
                    }

                    // 3. 切分文件
                    if (currentFileRows >= perfConfig.getSplitRows()) {
                        Long startLineNo = csvWriterContext.startLineNo;
                        csvWriterContext.close();
                        csvWriterContext = null;
                        fileIndex++;
                        // 保存切分记录到数据库
                        saveSplit(detailId, currentOutPath, startLineNo, currentFileRows);
                        currentFileRows = 0;
                    }
                }

                if (currentFileRows > 0) {
                    Long startLineNo = csvWriterContext.startLineNo;
                    csvWriterContext.close();
                    // 保存切分记录到数据库
                    saveSplit(detailId, currentOutPath, startLineNo, currentFileRows);
                }
            } finally {
                if (csvWriterContext != null) {
                    csvWriterContext.close();
                }
                if (errorWriter != null) errorWriter.close(); // 别忘了关闭错误文件流
            }
        }
    }

    private boolean isMigrationJobActive(Long qianyiId) {
        Qianyi qianyiById = qianyiRepo.findById(qianyiId).orElse(null);
        return migraRepo.findActiveStatusById(qianyiById.getJobId());
    }

    private CsvWriterContext createUtf8Writer(Path path, Long startLineNo) throws IOException {
        AppProperties.Performance performance = config.getPerformance();
        AppProperties.CsvDetailConfig utf8Split = config.getCsv().getUtf8Split();
        CsvWriterSettings settings = utf8Split.toWriterSettings();
        settings.setQuoteAllFields(true); // TDSQL 建议全引, 通常数据库导入建议全引用，或者加一个配置项控制

        Writer out = new BufferedWriter(new OutputStreamWriter(
                Files.newOutputStream(path), Charset.forName(utf8Split.getEncoding())),
                performance.getWriteBufferSize()); // 加大写缓冲
        return new CsvWriterContext(new CsvWriter(out, settings), startLineNo);
    }

    private class CsvWriterContext {
        private CsvWriter csvWriter;
        private Long startLineNo;
        public CsvWriterContext(CsvWriter csvWriter, Long startLineNo) {
            this.csvWriter = csvWriter;
            this.startLineNo = startLineNo;
        }
        public void close() {
            if (null != csvWriter) {
                csvWriter.close();
            }
        }
        public Long getStartLineNo() {
            return this.startLineNo;
        }
        public void write(String[] newRow) {
            this.write(newRow);
        }
    }

    private void throwException(long lineNo, String errorType, String[] row, List<ColumnErrorDetail> colErrors, Charset ibmCharset) {
        String[] errorInfo = formatErrorInfo(lineNo, errorType, row, colErrors, ibmCharset);
        throw new RuntimeException(Arrays.stream(errorInfo).collect(Collectors.joining("\n")));
    }

    /**
     * 写入错误记录
     */
    private void writeErrorRecord(CsvWriter errorWriter, long lineNo, String errorType, String[] row, List<ColumnErrorDetail> colErrors, Charset ibmCharset) {
        try {
            String[] errorInfo = formatErrorInfo(lineNo, errorType, row, colErrors, ibmCharset);
            errorWriter.writeRow(errorInfo);
        } catch (Exception e) {
            log.error("写入错误日志失败 Line:{}", lineNo, e);
        }
    }

    private String[] formatErrorInfo(long lineNo, String errorType, String[] row, List<ColumnErrorDetail> colErrors, Charset ibmCharset) {
        String rowBase64Ibm = "";
        String rowBase64Utf8 = "";
        String colDetailsJson = "";

        // 如果是行级错误（如列数不对），或者为了保留上下文，我们总是尝试计算整行的 Base64
        // 注意：这里的 Row_Base64_IBM 是"尝试还原"的值，不是物理原始值
        String rowString = toCsvString(row);
        rowBase64Ibm = Base64.getEncoder().encodeToString(rowString.getBytes(ibmCharset));
        rowBase64Utf8 = Base64.getEncoder().encodeToString(rowString.getBytes(StandardCharsets.UTF_8));

        // 如果是列级错误，生成详细的 JSON
        if (colErrors != null && !colErrors.isEmpty()) {
            colDetailsJson = gson.toJson(colErrors);
        }

        return new String[]{
                String.valueOf(lineNo),
                errorType,
                rowBase64Ibm,
                rowBase64Utf8,
                colDetailsJson
        };
    }

    /**
     * 核心验证逻辑
     */
    private List<ColumnErrorDetail> validateRowStability(String[] originalLine, String[] rowToWrite, Charset charset, CharsetEncoder encoder, boolean isTunneling) {
        List<ColumnErrorDetail> errors = new ArrayList<>();
        if (originalLine == null) return errors;

        for (int i = 0; i < rowToWrite.length; i++) {
            // 变量名改为 cell 更贴切
            String cellToWrite = rowToWrite[i];

            if (cellToWrite == null || cellToWrite.isEmpty()) continue;
            if (isPureAscii(cellToWrite)) continue; // 性能优化

            // 1. 显式检测替换字符
            if (cellToWrite.indexOf('\uFFFD') != -1) {
                errors.add(buildColumnError(i, cellToWrite, charset, "CONTAINS_REPLACEMENT_CHAR"));
                continue;
            }

            String originalCell = originalLine[i];

            // 2. 方案 B：双向回转验证
            // 修改了方法参数名，这里调用看起来更舒服
            if (!checkCellStability(originalCell, cellToWrite, charset, encoder, isTunneling)) {
                errors.add(buildColumnError(i, originalCell, charset, "STABILITY_MISMATCH"));
            }
        }
        return errors;
    }

    private ColumnErrorDetail buildColumnError(int index, String cellContent, Charset charset, String reason) {
        try {
            // 记录该列转换前的 Base64 (尝试还原)
            String ibmBase64 = Base64.getEncoder().encodeToString(cellContent.getBytes(charset));
            // 记录该列转换后的 Base64 (UTF8)
            String utf8Base64 = Base64.getEncoder().encodeToString(cellContent.getBytes(StandardCharsets.UTF_8));
            return new ColumnErrorDetail(index, reason, ibmBase64, utf8Base64);
        } catch (Exception e) {
            return new ColumnErrorDetail(index, "ENCODING_ERROR", "", "");
        }
    }
    /**
     * 单元格稳定性检查 (支持双模式)
     * @param originalCell 原始读取到的字符串 (如果开启 tunneling，这里是包含 \HEX\ 的串)
     * @param cellToWrite  准备写入的字符串 (如果开启 tunneling，这里是解析后的真字)
     */
    private boolean checkCellStability(String originalCell, String cellToWrite, Charset charset, CharsetEncoder encoder, boolean isTunneling) {
        try {
            if (isTunneling) {
                // --- 新模式：支持生僻字转义 (Pro版) ---
                // 模拟回转：尝试把真字 (cellToWrite) 变回大机格式
                // 逻辑：认识的字保留，不认识的字变回 \HEX\
                String restored = FastEscapeHandler.escapeForCheck(cellToWrite, encoder);

                // 比对：原始串 (含 \HEX\) vs 还原串 (含 \HEX\)
                return originalCell.equals(restored);

            } else {
                // --- 旧模式：标准回转 (Standard版) ---
                // 1. 强制转为字节
                byte[] bytes = cellToWrite.getBytes(charset);

                // 2. 转回字符串
                String restored = new String(bytes, charset);

                // 3. 比对
                return originalCell.equals(restored);
            }
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isPureAscii(String v) {
        for (int i = 0; i < v.length(); i++) {
            if (v.charAt(i) > 127) return false;
        }
        return true;
    }

    // 简单还原 CSV 行字符串，用于 Base64 生成
    private String toCsvString(String[] row) {
        if (row == null || row.length == 0) return "";
        // 简单用逗号拼接，仅做 Base64 参考用
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.length; i++) {
            sb.append(row[i]);
            if (i < row.length - 1) sb.append(",");
        }
        return sb.toString();
    }
    private CsvWriter createErrorWriter(Path path) throws IOException {
        CsvWriterSettings settings = new CsvWriterSettings();
        settings.getFormat().setDelimiter(',');
        settings.setQuoteAllFields(true); // 必须全引用，防止 JSON 破坏 CSV 结构
        settings.setMaxCharsPerColumn(100_000);
        Writer out = new BufferedWriter(new OutputStreamWriter(
                Files.newOutputStream(path), StandardCharsets.UTF_8));
        return new CsvWriter(out, settings);
    }

    @Data
    public static class ColumnErrorDetail {
        private int columnIndex;
        private String errorReason;
        private String ibm1388Base64; // 还原回IBM的Base64
        private String utf8Base64;    // 目标UTF8的Base64

        public ColumnErrorDetail(int columnIndex, String errorReason, String ibm1388Base64, String utf8Base64) {
            this.columnIndex = columnIndex;
            this.errorReason = errorReason;
            this.ibm1388Base64 = ibm1388Base64;
            this.utf8Base64 = utf8Base64;
        }
    }

    private void saveSplit(Long detailId, Path path, Long startLineNo, Long rowCount) {
        CsvSplit split = new CsvSplit();
        split.setDetailId(detailId);
        split.setSplitFilePath(path.toString());
        split.setStartRowNo(startLineNo);
        split.setRowCount(rowCount);
        split.setStatus(CsvSplitStatus.WL); // Wait Load
        splitRepo.save(split);
    }
}