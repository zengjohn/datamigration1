package com.example.moveprog.scheduler;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.dto.OkFileContent;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.BatchStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.example.moveprog.repository.QianyiRepository;
import com.example.moveprog.service.JdbcHelper;
import com.example.moveprog.service.TargetDatabaseConnectionManager;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 目录监控器
 */
@Component
@Slf4j
@RequiredArgsConstructor
@Validated
public class DirectoryMonitor {

    private final MigrationJobRepository jobRepo;
    private final QianyiRepository qianyiRepo;
    private final QianyiDetailRepository detailRepo;

    private final JdbcHelper jdbcHelper;
    private final TargetDatabaseConnectionManager targetDatabaseConnectionManager;

    private final AppProperties config;
    private final Gson gson = new Gson();

    @Scheduled(fixedDelay = 5000)
    public void scan() {
        // 1. 查找所有启用的 Job
        List<MigrationJob> jobs = jobRepo.findAll();

        for (MigrationJob job : jobs) {
            try {
                // 简单校验目录
                File dir = new File(job.getSourceDirectory());
                if (!dir.exists() || !dir.isDirectory()) {
                    log.warn("作业[{}] 监控目录不存在: {}", job.getName(), job.getSourceDirectory());
                    continue;
                }

                File[] okFiles = dir.listFiles((d, name) -> name.endsWith(".ok"));
                if (okFiles == null) continue;

                for (File okFile : okFiles) {
                    processOkFile(job, okFile);
                }
            } catch (Exception e) {
                log.error("扫描Job[{}]异常", job.getName(), e);
            }
        }
    }
    /**
     * 机器 A 只处理 机器 A 的文件
     * 处理单个 OK 文件
     * 无论成功失败，都尽量保存记录，以免重复扫描或丢失错误现场
     */
    @Transactional
    public void processOkFile(MigrationJob job, File okFile) {
        String myIp = config.getCurrentNodeIp();
        String okPath = okFile.getAbsolutePath();
        // 1. 防重：如果已经处理过（无论成功失败），直接跳过
        // 如果用户想重试 FAIL_PARSE 的任务，需要在界面点击“重试”（需配套接口删除旧记录）
        if (qianyiRepo.existsByOkFilePathAndNodeId(okPath, myIp)) return;

        log.info("发现新任务文件: {}", okPath);

        // 初始化对象（先不保存，等解析成功再保存，或者捕获异常保存失败状态）
        Qianyi qianyi = new Qianyi();
        // 【关键】自动绑定当前机器的 IP
        // 因为 DirectoryMonitor 只能扫到本机硬盘的文件，所以这个任务一定是本机的
        qianyi.setNodeId(myIp);
        qianyi.setJobId(job.getId());
        qianyi.setOkFilePath(okPath);
        // 默认表名：如果 JSON 解析挂了，用文件名兜底
        String defaultTableName = okFile.getName().replace(".ok", "");
        qianyi.setTableName(defaultTableName);
        qianyi.setDdlFilePath(""); // 暂时置空

        try {
            // 2. 读取并解析 JSON
            String jsonContent;
            try {
                jsonContent = Files.readString(okFile.toPath());
            } catch (Exception e) {
                throw new RuntimeException("无法读取文件: " + e.getMessage());
            }

            OkFileContent content;
            try {
                content = gson.fromJson(jsonContent, OkFileContent.class);
            } catch (JsonSyntaxException e) {
                throw new RuntimeException("文件内容不是有效的 JSON 格式");
            }

            if (content == null) {
                throw new RuntimeException("JSON 内容为空");
            }

            // 3. 处理路径 (支持相对路径) & 校验文件存在
            // 获取 ok 文件所在的目录，作为相对路径的基准
            Path basePath = okFile.getParentFile().toPath();

            // 3.1 校验 DDL
            if (content.ddl == null || content.ddl.trim().isEmpty()) {
                throw new RuntimeException("JSON 中缺少 'ddl' 字段");
            }
            File ddlFile = resolveFile(basePath, content.ddl);
            if (!ddlFile.exists()) {
                throw new RuntimeException("找不到 DDL 文件: " + ddlFile.getAbsolutePath());
            }


            String finalDdlPath = ddlFile.getAbsolutePath();
            // 更新表名 (使用 DDL 文件名)
            String realTableName = ddlFile.getName().replace(".sql", "");
            String[] splits = StringUtils.split(realTableName, "-");
            if (splits.length == 1) {
                qianyi.setSchemaName(null);
                qianyi.setTableName(splits[0]);
            } else {
                qianyi.setSchemaName(splits[0]);
                qianyi.setTableName(splits[1]);
            }
            if (Objects.nonNull(content.schema) && !content.schema.trim().isEmpty()) {
                qianyi.setSchemaName(content.schema.trim());
            }
            if (Objects.nonNull(content.table) && !content.table.trim().isEmpty()) {
                qianyi.setTableName(content.table.trim());
            }

            qianyi.setDdlFilePath(finalDdlPath);

            // 3.2 校验 CSV 列表
            if (content.csv == null || content.csv.isEmpty()) {
                throw new RuntimeException("JSON 中 'csv' 列表为空");
            }
            for (int i=0; i<content.csv.size(); i++) {
                String csv = content.csv.get(i);
                if (Objects.isNull(csv) || csv.isEmpty()) {
                    throw new RuntimeException("JSON 中 第 "+ (i+1) + "个 'csv' 为空");
                }
            }

            List<String> finalCsvPaths = new ArrayList<>();
            for (String csvRelPath : content.csv) {
                File csvFile = resolveFile(basePath, csvRelPath);
                if (!csvFile.exists()) {
                    throw new RuntimeException("找不到数据文件: " + csvFile.getAbsolutePath());
                }
                finalCsvPaths.add(csvFile.getAbsolutePath());
            }


            boolean exists = checkTargetTableExists(job.getId(), qianyi.getSchemaName(), qianyi.getTableName());
            if (!exists) {
                throw new RuntimeException("目标表" + jdbcHelper.tableNameQuote(qianyi.getSchemaName(), qianyi.getTableName()) + "不存在, 前先在目标端建表");
            }

            // 4. 一切正常，保存主记录
            qianyi.setStatus(BatchStatus.PROCESSING);
            qianyi.setErrorMsg(null);
            qianyiRepo.save(qianyi);


            // 5. 保存明细记录
            for (String absCsvPath : finalCsvPaths) {
                QianyiDetail detail = new QianyiDetail();
                // 【关键】明细也绑定本机 IP
                detail.setNodeId(myIp);
                detail.setJobId(qianyi.getJobId());
                detail.setQianyiId(qianyi.getId());
                detail.setSourceCsvPath(absCsvPath);
                detail.setStatus(DetailStatus.NEW);
                detail.setProgress(0);
                detailRepo.save(detail);
            }

            log.info("任务解析成功: ID={}, 表={}, 文件数={}", qianyi.getId(), realTableName, finalCsvPaths.size());

        } catch (Exception e) {
            log.error("解析 OK 文件失败 [{}]: {}", okPath, e.getMessage());

            // 6. 异常流程：保存为失败状态
            qianyi.setStatus(BatchStatus.FAIL_PARSE);
            qianyi.setErrorMsg(e.getMessage());
            // 如果 ddlFilePath 还没解析出来，给个默认值防止入库报错(如果是 not null)
            if (qianyi.getDdlFilePath() == null || qianyi.getDdlFilePath().isEmpty()) {
                qianyi.setDdlFilePath("UNKNOWN");
            }
            qianyiRepo.save(qianyi);
        }
    }

    private boolean checkTargetTableExists(Long jobId, String schema, String table) throws SQLException {
        String checkExistsTableSql = jdbcHelper.checkExistsTableSql(schema, table);
        try(Connection connection = targetDatabaseConnectionManager.getConnection(jobId, true)) {
            try(Statement statement = connection.createStatement()) {
                try(ResultSet resultSet = statement.executeQuery(checkExistsTableSql)) {
                    while (resultSet.next()) {
                        return resultSet.getLong(1) > 0L;
                    }
                }
            }
        }
        return false;
    }

    /**
     * 辅助方法：解析文件路径
     * 如果是绝对路径，直接返回；如果是相对路径，基于 basePath 拼接
     */
    private File resolveFile(Path basePath, String pathStr) {
        Path p = Paths.get(pathStr);
        if (p.isAbsolute()) {
            return p.toFile();
        } else {
            return basePath.resolve(pathStr).toFile();
        }
    }
}