package com.example.moveprog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling // <--- 关键！开启定时任务支持，否则调度器不工作
@EnableAsync // <--- 必须确认有这个！
public class BigCsvApplication {

    public static void main(String[] args) {
        // 启动 Spring 容器，程序会持续运行等待定时任务触发
        SpringApplication.run(BigCsvApplication.class, args);
    }
}