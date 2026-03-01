package com.example.moveprog.service;

import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiRepository;
import com.example.moveprog.util.MigrationOutputDirectorUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

@Component
@Slf4j
@RequiredArgsConstructor
public class MigrationArtifactManager {
    private final MigrationJobRepository jobRepository;
    private final QianyiRepository qianyiRepository;
    private final CsvSplitRepository splitRepo;

    /**
     * 清除验证阶段产生的差异文件 (用于 Reverify 和 Reload)
     */
    public void cleanVerifyArtifacts(CsvSplit csvSplit) {
        try {
            Long jobId = csvSplit.getJobId();
            MigrationJob migrationJob = jobRepository.findById(jobId).orElseThrow();

            // 删除差异内容文件 (.diff)
            String verifyResultFile = MigrationOutputDirectorUtil.verifyResultFile(migrationJob, csvSplit.getQianyiId(), csvSplit.getId());
            boolean result = Files.deleteIfExists(Paths.get(verifyResultFile));
            if (result) {
                log.info("已清理差异文件: {}", verifyResultFile);
            }
        } catch (Exception e) {
            log.warn("清理差异文件失败: {}", csvSplit.getId(), e);
        }
    }

    public void deleteEmptyVerifyResultFile(CsvSplit csvSplit) throws IOException {
        Long jobId = csvSplit.getJobId();
        MigrationJob migrationJob = jobRepository.findById(jobId).orElseThrow();

        String verifyResultFile = MigrationOutputDirectorUtil.verifyResultFile(migrationJob, csvSplit.getQianyiId(), csvSplit.getId());
        File diffFile = new File(verifyResultFile);
        if (diffFile.exists() && diffFile.length() == 0) {
            diffFile.delete();
        }
    }

    /**
     * 清除转码阶段产生的文件
     */
    public void cleanTranscodeArtifacts(QianyiDetail detail) {
        try {
            List<CsvSplit> csvSplits = splitRepo.findByDetailId(detail.getId());

            if (Objects.nonNull(csvSplits)) {
                for (CsvSplit csvSplit : csvSplits) {
                    String splitFilePath = csvSplit.getSplitFilePath();
                    boolean result = Files.deleteIfExists(Paths.get(splitFilePath));
                    if (result) {
                        log.info("已清理拆分文件: {}", splitFilePath);
                    }

                    String patchFilePath = MigrationOutputDirectorUtil.getPatchSplitPath(csvSplit);
                    result = Files.deleteIfExists(Paths.get(patchFilePath));
                    if (result) {
                        log.info("已清理patch文件: {}", patchFilePath);
                    }
                }
            }

            Long jobId = detail.getJobId();
            MigrationJob migrationJob = jobRepository.findById(jobId).orElseThrow();

            String errorFile = MigrationOutputDirectorUtil.transcodeErrorFile(migrationJob, detail.getQianyiId(), detail.getId());
            boolean result = Files.deleteIfExists(Paths.get(errorFile));
            if (result) {
                log.info("已清理拆分失败文件: {}", errorFile);
            }
        } catch (Exception e) {
            log.warn("清理拆分文件失败: {}", detail.getId(), e);
        }
    }

    /**
     * 【重磅】验证通过后的清理 (节省磁盘)
     */
    public void cleanQianyiDetailArtifacts(QianyiDetail detail) {
        try {
            List<CsvSplit> csvSplits = splitRepo.findByDetailId(detail.getId());
            if (Objects.nonNull(csvSplits)) {
                for (CsvSplit csvSplit : csvSplits) {
                    cleanVerifyArtifacts(csvSplit);
                }
            }

            cleanTranscodeArtifacts(detail);

            log.info("已清理切片文件中间以释放磁盘: {}", detail.getId());
        } catch (Exception e) {
            log.warn("清理切片文件失败: {}", detail.getId(), e);
        }
    }

    /**
     * 批次级终极清理 (Deep Clean)
     * 场景：结单成功后，清理该批次下的所有物理文件
     */
    public void cleanBatchArtifacts(Long qianyiId) {
        Qianyi qianyi = qianyiRepository.findById(qianyiId).orElse(null);
        if (Objects.isNull(qianyi)) {
            return;
        }
        MigrationJob migrationJob = jobRepository.findById(qianyi.getJobId()).orElse(null);
        if (Objects.isNull(migrationJob)) {
            return;
        }

        log.info("开始执行批次[{}]的终极清理...", qianyiId);
        String batchOutDirStr = MigrationOutputDirectorUtil.batchOutDirectory(migrationJob, qianyiId);
        Path batchOutPath = Paths.get(batchOutDirStr).toAbsolutePath().normalize();
        // 【新增】防御性检查：再次确认要删除的目录是否安全
        // 确保它确实包含 qianyiId，防止 MigrationOutputDirectorUtil 逻辑变更导致删错父目录
        if (!batchOutPath.endsWith(qianyiId.toString())) {
            log.error("安全阻断：试图删除的目录名[{}]与批次ID[{}]不匹配，放弃操作。", batchOutPath, qianyiId);
            return;
        }

        // 确保路径深度足够 (例如至少 /data/output/101，path count >= 2)
        if (batchOutPath.getNameCount() < 2) {
            log.error("安全阻断：试图删除的目录[{}]层级过浅，放弃操作。", batchOutPath);
            return;
        }

        try {
            boolean result = FileSystemUtils.deleteRecursively(batchOutPath);
            if (result){
            log.info("批次[{}] 物理文件清理完成, 目录 {} 被删除", qianyiId, batchOutPath);
            }
        } catch (IOException e) {
            log.error("清理批次文件失败", e);
        }
    }

}
