package com.example.moveprog.integration;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;

import java.util.Map;

@SpringBootTest
@ActiveProfiles("test")
public abstract class BaseIntegrationTest {

    // 1. 去掉 @Container 注解，直接声明静态变量
    protected static final MySQLContainer<?> MYSQL_CONTAINER = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("test1")
            .withUsername("root")
            .withPassword("111111")
            .withReuse(true) // 开启复用
            // 核心大招：把数据库引擎放在内存里跑，完全绕过机械硬盘！
            .withTmpFs(Map.of("/var/lib/mysql", "rw"));

    protected static final MySQLContainer<?> TARGET_CONTAINER = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("test2")
            .withUsername("test")
            .withPassword("testpasswd")
            .withReuse(true) // 开启复用
            // 核心大招：把数据库引擎放在内存里跑，完全绕过机械硬盘！
            .withTmpFs(Map.of("/var/lib/mysql", "rw"));

    // 2. 使用静态代码块，在整个 JVM 生命周期中只手动 start() 一次
    static {
        MYSQL_CONTAINER.start();
        TARGET_CONTAINER.start();
    }

    // 3. 动态注入数据源属性
    @DynamicPropertySource
    static void registerMySQLProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL_CONTAINER::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL_CONTAINER::getUsername);
        registry.add("spring.datasource.password", MYSQL_CONTAINER::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("spring.jpa.defer-datasource-initialization", () -> "true");
    }
}