package com.example.moveprog.scheduler;

import com.example.moveprog.dto.OkFileContent;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.BatchStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.example.moveprog.repository.QianyiRepository;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class DirectoryMonitor {

    private final MigrationJobRepository jobRepo;
    private final QianyiRepository qianyiRepo;
    private final QianyiDetailRepository detailRepo;
    private final Gson gson = new Gson();

    @Scheduled(fixedDelay = 5000)
    public void scan() {
        // 1. 查找所有启用的 Job
        List<MigrationJob> jobs = jobRepo.findAll();

        for (MigrationJob job : jobs) {
            try {
                File dir = new File(job.getSourceDirectory());
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

    @Transactional // 保证原子性：Qianyi 和 Detail 要么都生，要么都不生
    public void processOkFile(MigrationJob job, File okFile) {
        String okPath = okFile.getAbsolutePath();
        if (qianyiRepo.existsByOkFilePath(okPath)) return; // 查重

        try {
            // 1. 解析 JSON
            String jsonContent = Files.readString(okFile.toPath());
            OkFileContent content = gson.fromJson(jsonContent, OkFileContent.class);

            // 2. 解析表名 (假设 DDL 文件名就是表名，如 /path/to/T_USER_01.sql)
            String tableName = new File(content.ddl).getName().replace(".sql", ""); // 简单处理

            // 3. 创建 Qianyi (批次)
            Qianyi qianyi = new Qianyi();
            qianyi.setJobId(job.getId());
            qianyi.setTableName(tableName);
            qianyi.setOkFilePath(okPath);
            qianyi.setDdlFilePath(content.ddl);
            qianyi.setStatus(BatchStatus.PROCESSING);
            qianyiRepo.save(qianyi);

            // 4. 创建 QianyiDetail (每个 CSV 一个任务)
            if (content.csv != null) {
                for (String csvPath : content.csv) {
                    QianyiDetail detail = new QianyiDetail();
                    detail.setJobId(qianyi.getJobId());
                    detail.setQianyiId(qianyi.getId());
                    detail.setTableName(tableName);
                    detail.setSourceCsvPath(csvPath);
                    detail.setStatus(DetailStatus.NEW); // 初始状态
                    detail.setProgress(0);
                    detailRepo.save(detail);
                }
            }
            
            log.info("生成批次任务: ID={}, 表={}, 文件数={}", qianyi.getId(), tableName, content.csv.size());

        } catch (Exception e) {
            log.error("解析OK文件失败: {}", okPath, e);
            // 这里可以移走坏的ok文件到 error 目录
        }
    }
}