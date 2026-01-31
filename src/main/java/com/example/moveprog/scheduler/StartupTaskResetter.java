package com.example.moveprog.scheduler;

import com.example.moveprog.config.AppProperties;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 启动时的"清洁工"
 * 作用：处理上次意外停机导致的"僵尸任务"
 */
@Component
@Slf4j
public class StartupTaskResetter implements ApplicationRunner {

    @Autowired private QianyiDetailRepository detailRepo;
    @Autowired private CsvSplitRepository splitRepo;
    @Autowired private AppProperties appProperties;

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        log.info(">>> 系统启动，开始检查异常中断的任务...");

        String currentNodeIp = appProperties.getCurrentNodeIp();

        // 1. 恢复 Detail: TRANSCODING -> NEW
        // 意味着上次转码没做完就挂了，必须重头再来
        int countDetail = detailRepo.resetStatus(DetailStatus.TRANSCODING, DetailStatus.NEW, currentNodeIp);
        if (countDetail > 0) {
            log.warn("检测到 {} 个文件在转码中途崩溃，已重置为 NEW 等待重新调度。", countDetail);
        }

        // 2. 恢复 Split: LOADING -> WAIT_LOAD
        // 意味着装载线程挂了，重置回去让调度器重新捡起来
        int countLoad = splitRepo.resetStatus(CsvSplitStatus.LOADING, CsvSplitStatus.WAIT_LOAD, currentNodeIp);
        if (countLoad > 0) {
            log.warn("检测到 {} 个分片在装载中途崩溃，已重置为 WAIT_LOAD。", countLoad);
        }

        // 3. 恢复 Split: VERIFYING -> WAIT_VERIFY
        int countVerify = splitRepo.resetStatus(CsvSplitStatus.VERIFYING, CsvSplitStatus.WAIT_VERIFY, currentNodeIp);
        if (countVerify > 0) {
            log.warn("检测到 {} 个分片在校验中途崩溃，已重置为 WAIT_VERIFY。", countVerify);
        }
        
        log.info("<<< 异常任务清理完毕，调度器准备就绪。");
    }
}