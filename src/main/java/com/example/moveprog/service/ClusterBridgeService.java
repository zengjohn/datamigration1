package com.example.moveprog.service;

import com.example.moveprog.config.AppProperties;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RestTemplate;

import java.util.Enumeration;
import java.util.function.Supplier;

@Service
@Slf4j
public class ClusterBridgeService {
    private final AppProperties config;

    private final int serverPort;

    private final HttpServletRequest request;
    // 如果没有配置 RestTemplate Bean，可以直接 new 一个
    private final RestTemplate restTemplate = new RestTemplate();


    // Spring 看到这里需要 Environment，会自动把容器里的那个 Env 传进来
    // 这个 Environment 里有什么？
    // Spring 会自动把以下内容都封装进这个对象里，供您查询：
    //   application.yml / application.properties 里的所有配置。
    //   系统环境变量 (System Environment Variables)。
    //   JVM 启动参数 (-Dserver.port=9090 等)。
    //   命令行参数 (--server.port=9090)。
    public ClusterBridgeService(AppProperties config, Environment env, HttpServletRequest request) {
        this.config = config;

        // 从 Environment 中手动读取，这样 serverPort 就可以是 final 的了
        this.serverPort = env.getProperty("server.port", Integer.class, 8080);

        /**
         * 特别说明：为什么单例 Service 能注入 Request？
         * 您可能会疑惑：ClusterBridgeService 是**单例（Singleton）的（应用启动只创建一次），而 request 是请求级（Request Scope）**的（每个用户请求都不一样），把一个短命的对象注入给长命的对象，不会出问题吗？
         * Spring 的魔法：代理（Proxy）
         *    构造函数里注入进来的这个 this.request，并不是真正的 Request 对象，而是一个 “空壳代理” (Proxy)。
         *    当您在业务代码里调用 request.getHeader(...) 时，这个代理会去 当前线程 (ThreadLocal) 里找到属于当前请求的那个真正的 Request 对象，并把调用转发给它。
         * 结论：您可以放心地在单例 Service 中使用它，它是线程安全的。
         */
        this.request = request;
    }

    /**
     * 【核心方法】执行或转发
     * @param targetNodeIp 目标机器的 IP (从数据库查出来的)
     * @param localLogic   如果是本机，要执行的业务逻辑 (Lambda 表达式)
     * @return 统一返回 ResponseEntity
     */
    public ResponseEntity<?> runOrProxy(String targetNodeIp, Supplier<ResponseEntity<?>> localLogic) {

        // 1. 判空或判断是否本机 (兼容 null 为本机，视业务而定)
        if (targetNodeIp.equals(config.getCurrentNodeIp())) {
            // >>> 命中本机：直接执行业务逻辑 <<<
            return localLogic.get();
        }

        // 2. >>> 命中异地：执行反向代理 <<<
        try {
            return proxyRequest(targetNodeIp);
        } catch (Exception e) {
            log.error("集群转发失败: Target={}", targetNodeIp, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("无法连接到计算节点: " + targetNodeIp + ", 错误: " + e.getMessage());
        }
    }

    private ResponseEntity<?> proxyRequest(String targetIp) throws Exception {
        // A. 构造远程 URL (复用当前的 URI 和 QueryString)
        String queryString = request.getQueryString();
        String uri = request.getRequestURI();
        String url = String.format("http://%s:%d%s%s", 
                targetIp, serverPort, uri, (queryString == null ? "" : "?" + queryString));

        log.info(">> 转发请求到: {}", url);

        // B. 复制请求头 (把 Cookie, Content-Type 等透传过去)
        HttpHeaders headers = new HttpHeaders();
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String name = headerNames.nextElement();
            // Host 头不能透传，否则对方会认为是发给 Nginx 的
            if (!"host".equalsIgnoreCase(name)) {
                headers.add(name, request.getHeader(name));
            }
        }

        // C. 复制请求体 (如果是 POST)
        byte[] body = StreamUtils.copyToByteArray(request.getInputStream());
        HttpEntity<byte[]> httpEntity = new HttpEntity<>(body, headers);

        // D. 发起请求
        // 使用 byte[].class 接收，这样无论是文本还是文件流都能通用处理
        return restTemplate.exchange(url, HttpMethod.valueOf(request.getMethod()), httpEntity, byte[].class);
    }
}