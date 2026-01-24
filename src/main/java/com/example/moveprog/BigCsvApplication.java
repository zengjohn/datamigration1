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
        try {
            // 启动 Spring 容器，程序会持续运行等待定时任务触发
            SpringApplication.run(BigCsvApplication.class, args);
        } catch (Throwable e) {
            // 【新增】捕获所有启动异常（包括配置错误），打印到控制台
            // 配合修改后的 start.sh，这里的输出会被记录到 start.out 中
            System.err.println("=========================================");
            System.err.println("FATAL ERROR: 应用启动失败！");
            System.err.println("可能是 application.yml 配置有误或端口被占用。");
            System.err.println("=========================================");
            e.printStackTrace();
        }
    }
}