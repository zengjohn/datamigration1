package com.example.moveprog.config;
// 建议放在 config 或 support 包下

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import javax.sql.DataSource;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.sql.Connection;
import java.util.Enumeration;

@Component
@Order(1) // 保证它最先执行 (优先级高)
@Slf4j
@RequiredArgsConstructor
public class StartupValidator implements ApplicationRunner {
    private final AppProperties config;
    private final DataSource dataSource;

    @Override
    public void run(ApplicationArguments args) {
        log.info(">>> 开始应用启动自检...");

        try {
            if (!isLocalIP(config.getCurrentNodeIp())) {
                throw new RuntimeException("currentNodeIp 必须配置本机地址");
            }

            // 校验数据库连接
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

    public static boolean isLocalIP(String ip) {
        try {
            InetAddress addr = InetAddress.getByName(ip);
            if (addr.isLoopbackAddress()) return false;

            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface iface = networkInterfaces.nextElement();
                if (!iface.isUp() || iface.getDisplayName().toLowerCase().contains("virtual")) {
                    continue;
                }

                Enumeration<InetAddress> inetAddresses = iface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (addr.equals(inetAddress)) {
                        return true;
                    }
                }
            }
            return false;
        } catch (Exception e) {
        }
        return false;
    }


}