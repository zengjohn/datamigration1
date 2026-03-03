package com.example.moveprog.controller;

import com.example.moveprog.scheduler.MigrationDispatcher;
import com.example.moveprog.service.ClusterBridgeService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * 系统级动态配置 (运维专用)
 */
@RestController
@RequestMapping("/api/sys/config")
@RequiredArgsConstructor
@Slf4j
public class SystemConfigController {

    private final MigrationDispatcher dispatcher;
    private final ClusterBridgeService bridgeService; // 注入 BridgeService
    private final MeterRegistry registry; // 【新增】注入 Registry

    /**
     * 获取当前机器的并发配置
     */
    @GetMapping("/concurrency")
    public ResponseEntity<Map<String, Integer>> getConcurrency() {
        Map<String, Integer> status = new HashMap<>();
        status.put("load", dispatcher.getCurrentLoadLimit());
        status.put("verify", dispatcher.getCurrentVerifyLimit());
        return ResponseEntity.ok(status);
    }

    /**
     * 修改当前机器的并发限制
     */
    @PostMapping("/concurrency")
    public ResponseEntity<?> updateConcurrency(@RequestBody Map<String, Integer> payload) {
        Integer load = payload.get("load");
        Integer verify = payload.get("verify");

        if (load != null) {
            dispatcher.resizeLoadPermits(load);
        }
        if (verify != null) {
            dispatcher.resizeVerifyPermits(verify);
        }

        return ResponseEntity.ok(Map.of(
                "message", "配置已更新",
                "currentLoad", dispatcher.getCurrentLoadLimit(),
                "currentVerify", dispatcher.getCurrentVerifyLimit()
        ));
    }

    /**
     * 【新增】获取本机详细监控快照 (供管理端聚合使用)
     * 包含: CPU, Mem, Thread, 以及所有(Meta+Target)连接池状态
     */
    @GetMapping("/node-monitor")
    public ResponseEntity<Map<String, Object>> getNodeInternalStatus() {
        Map<String, Object> metrics = new HashMap<>();

        // 1. 系统基础指标 (安全获取，防止报错)
        metrics.put("cpu", getGaugeValue("system.cpu.usage", null));

        Map<String, Double> mem = new HashMap<>();
        mem.put("used", getGaugeValue("jvm.memory.used", "area:heap"));
        mem.put("max", getGaugeValue("jvm.memory.max", "area:heap"));
        // 【新增】获取已提交内存 (Committed)，这更接近任务管理器看到的数值
        mem.put("committed", getGaugeValue("jvm.memory.committed", "area:heap"));
        metrics.put("mem", mem);

        metrics.put("threads", getGaugeValue("jvm.threads.live", null));

        // 2. 并发配置
        metrics.put("config", Map.of(
                "load", dispatcher.getCurrentLoadLimit(),
                "verify", dispatcher.getCurrentVerifyLimit()
        ));

        // 3. 【核心】自动发现所有 HikariCP 连接池
        // Micrometer 会为每个池子生成 metrics，我们通过 tag 遍历
        List<Map<String, Object>> pools = new ArrayList<>();

        // 查找所有名为 hikaricp.connections.active 的指标
        Collection<Gauge> gauges = registry.find("hikaricp.connections.active").gauges();

        for (Gauge activeGauge : gauges) {
            String poolName = activeGauge.getId().getTag("pool");
            if (poolName == null) continue;

            // 根据 poolName 构造其他指标的查询
            double active = activeGauge.value();
            double idle = getGaugeValue("hikaricp.connections.idle", "pool:" + poolName);
            double max = getGaugeValue("hikaricp.connections.max", "pool:" + poolName);
            double pending = getGaugeValue("hikaricp.connections.pending", "pool:" + poolName);

            Map<String, Object> poolStat = new HashMap<>();
            poolStat.put("name", poolName);
            poolStat.put("active", (int) active);
            poolStat.put("idle", (int) idle);
            poolStat.put("max", (int) max);
            poolStat.put("pending", (int) pending);

            // 标记类型：是元数据库还是目标作业库
            poolStat.put("type", poolName.contains("Job-") ? "TARGET" : "META");

            pools.add(poolStat);
        }

        // 排序：Meta 在前，Job 在后
        pools.sort(Comparator.comparing(m -> (String)m.get("type")));
        metrics.put("pools", pools);

        return ResponseEntity.ok(metrics);
    }

    // 辅助方法
    private double getGaugeValue(String name, String tag) {
        try {
            Search search = registry.find(name);
            if (tag != null) {
                String[] parts = tag.split(":");
                search.tag(parts[0], parts[1]);
            }

            Collection<Gauge> gauges = search.gauges();
            if (gauges.isEmpty()) return -1.0;

            return gauges.stream()
                    .mapToDouble(Gauge::value)
                    // 过滤掉 -1 (未定义的 Max 值) 或异常负值
                    .filter(v -> v >= 0)
                    .sum();
        } catch (Exception e) {
            return -1.0;
        }
    }

    /**
     * 【新增】获取集群所有节点状态
     */
    @GetMapping("/cluster/status")
    public ResponseEntity<List<Map<String, Object>>> getClusterStatus() {
        return ResponseEntity.ok(bridgeService.aggregateClusterMetrics());
    }

    /**
     * 【新增】广播修改集群并发配置
     */
    @PostMapping("/cluster/concurrency")
    public ResponseEntity<?> updateClusterConcurrency(@RequestBody Map<String, Integer> payload) {
        bridgeService.broadcastConfig(payload);
        return ResponseEntity.ok("广播指令已发送，请稍后刷新状态");
    }

}