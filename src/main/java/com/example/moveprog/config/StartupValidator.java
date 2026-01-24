package com.example.moveprog.config;
// 建议放在 config 或 support 包下

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;

@Component
@Order(1) // 保证它最先执行 (优先级高)
@Slf4j
@RequiredArgsConstructor
public class StartupValidator implements ApplicationRunner {

    private final AppProperties appProperties;
    private final DataSource dataSource;

    @Override
    public void run(ApplicationArguments args) {
        log.info(">>> 开始应用启动自检...");

        try {
            // 1. 调用 AppProperties 里的 validate (或者在这里直接校验)
            validateDirectories();

            // 2. 校验数据库连接
            validateDatabase();

            log.info("<<< 应用自检通过，服务正常启动。");

        } catch (Exception e) {
            log.error("************************************************************");
            log.error("FATAL ERROR: 应用启动自检失败，程序将退出！");
            log.error("错误详情: {}", e.getMessage());
            log.error("************************************************************");

            // 强制退出 JVM (非 0 状态码表示异常退出)
            System.exit(1);
        }
    }

    /**
     * 校验配置目录
     */
    private void validateDirectories() {
        // 获取配置的目录
        String outputDir = appProperties.getJob().getOutputDir();
        String errorDir = appProperties.getJob().getErrorDir();
        String verifyResultDir = appProperties.getVerify().getVerifyResultBasePath();

        // 你的 AppProperties 可能还有其他需要校验的路径
        
        checkDir("Output Directory", outputDir);
        checkDir("Error Directory", errorDir);
        checkDir("Error verifyResult Directory", verifyResultDir);
    }

    private void checkDir(String name, String path) {
        if (path == null || path.trim().isEmpty()) {
            throw new RuntimeException(name + " 未配置，请检查 application.yml");
        }
        File dir = new File(path);
        if (!dir.exists()) {
            // 策略 A: 自动创建 (推荐)
            boolean created = dir.mkdirs();
            if (created) {
                log.info("目录不存在，已自动创建: {}", path);
            } else {
                throw new RuntimeException("目录不存在且无法自动创建: " + path + " (请检查权限)");
            }
        } else if (!dir.isDirectory()) {
            throw new RuntimeException(name + " 路径被占用且不是文件夹: " + path);
        } else if (!dir.canWrite()) {
            throw new RuntimeException(name + " 存在但无写权限: " + path);
        }
    }

    private void validateEnumConstant() {

    }

    /**
     * 校验数据库连接
     */
    private void validateDatabase() {
        try (Connection conn = dataSource.getConnection()) {
            if (!conn.isValid(5)) { // 5秒超时
                throw new RuntimeException("数据库连接无效");
            }
            log.info("数据库连接正常: {}", conn.getMetaData().getURL());
        } catch (Exception e) {
            throw new RuntimeException("无法连接到数据库，请检查 URL/User/Password 配置。(" + e.getMessage() + ")");
        }
    }
}