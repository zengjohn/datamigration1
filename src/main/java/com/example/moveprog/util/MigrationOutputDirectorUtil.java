package com.example.moveprog.util;

import java.io.IOException;
import java.nio.file.*;

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
     * 转移作业 outDirectory 输出目录
     * @param outDirectory 输出目录
     * @return
     */
    public static String transcodeErrorDirectory(String outDirectory) {
        return Paths.get(outDirectory, "output_error").toString();
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
     * @param outDirectory
     * @return
     */
    public static String transcodeSplitDirectory(String outDirectory) {
        return Paths.get(outDirectory, "output_split").toString();
    }

    /**
     * 转码时生产文件名
     * @param transcodeSplitDirectory 转码拆分目录
     * @param detailId 明细Id
     * @param fileIndex 拆分序号
     * @return
     */
    public static String transcodeSplitFile(String transcodeSplitDirectory, Long detailId, int fileIndex) {
        return Paths.get(transcodeSplitDirectory, detailId + "_" + fileIndex + ".csv").toString();
    }

    /**
     * 验证结果输出目录
     * @param outDirectory 转移作业outDirectory输出目录
     * @return
     */
    public static String verifyResultDirectory(String outDirectory) {
        return Paths.get(outDirectory, "verify_result").toString();
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
