package com.example.moveprog.util;

import com.example.moveprog.entity.MigrationJob;

import java.io.IOException;
import java.nio.file.*;

/**
 * 输出目录管理
 * 为了避免目录名代码散布在各个地方，统一管理目录名
 */
public abstract class MigrationOutputDirectorUtil {

    public static void beSureNewDirectory(Path directory) {
        if (Files.isDirectory(directory)) {
            try {
                if (!isEmptyDirectory(directory)) {
                    throw new RuntimeException("输出目录不是空目录");
                }
            } catch (IOException e) {
                throw new RuntimeException("检查输出目录时报错");
            }
            return;
        }

        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new RuntimeException("创建输出目录时报错");
        }
    }


    /**
     * 某个批次中间文件输出目录
     * @param migrationJob 整个job的输出目录
     * @param qianyiId 批次(对于一个ok文件)的输出目录
     * @return 某个批次产生的文件的根目录：输出目录(对于jobId)\迁移Id
     */
    public static String batchOutDirectory(MigrationJob migrationJob, Long qianyiId) {
        String outDirectory = migrationJob.getOutDirectory();
        return Paths.get(outDirectory, qianyiId.toString()).toString();
    }

    /**
     * 转移作业 outDirectory 输出目录
     * @param migrationJob 输出目录
     * @param qianyiId 迁移批次(对于一个ok文件)Id
     * @return
     */
    public static String transcodeErrorDirectory(MigrationJob migrationJob, Long qianyiId) {
        return Paths.get(batchOutDirectory(migrationJob, qianyiId), "output_error").toString();
    }

    /**
     * 保存转码失败的文件名
     * @param transcodeErrorDirectory 转码失败目录
     * @param detailId 要转码的明细Id
     * @return
     */
    public static String transcodeErrorFile(String transcodeErrorDirectory, Long detailId) {
        return Paths.get(transcodeErrorDirectory, detailId + "_error.csv").toString();
    }

    /**
     * 转移作业 outDirectory 输出目录
     * @param migrationJob
     * @param qianyiId 迁移批次(对于一个ok文件)Id
     * @return
     */
    public static String transcodeSplitDirectory(MigrationJob migrationJob, Long qianyiId) {
        return Paths.get(batchOutDirectory(migrationJob, qianyiId), "output_split").toString();
    }

    public static Path transcodeSplitResultDirectory(String transcodeSplitDirectory, Long detailId) {
        // 1. 构造子目录路径
        return Paths.get(transcodeSplitDirectory, String.valueOf(detailId));
    }

    /**
     * 转码时生产文件名
     * @param transcodeSplitDirectory 转码拆分目录
     * @param detailId 明细Id
     * @param fileIndex 拆分序号
     * @return
     */
    public static String transcodeSplitFile(String transcodeSplitDirectory, Long detailId, int fileIndex) {
        Path detailDir = transcodeSplitResultDirectory(transcodeSplitDirectory, detailId);
        // 【关键】确保这个子目录存在 (虽然 Files.write 会报错，但建议工具类里或者 Service 里保证创建)
        // 为了代码纯粹性，这里只返回路径字符串。创建目录的动作交给 Service。
        return detailDir.resolve(fileIndex + ".csv").toString();
    }

    /**
     * 验证结果输出目录
     * @param migrationJob 转移作业outDirectory输出目录
     * @param qianyiId 迁移批次(对于一个ok文件)Id
     * @return
     */
    public static String verifyResultDirectory(MigrationJob migrationJob, Long qianyiId) {
        return Paths.get(batchOutDirectory(migrationJob, qianyiId), "verify_result").toString();
    }

    public static String verifyResultFile(String verifyResultPath, Long splitId) {
        return Paths.get(verifyResultPath, "split_" + splitId + "_diff.txt").toString();
    }

    private static boolean isEmptyDirectory(Path directory) throws IOException {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
            return !dirStream.iterator().hasNext();
        }
    }

}
