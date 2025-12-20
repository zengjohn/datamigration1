package com.example.moveprog.scheduler;

import com.example.moveprog.entity.Qianyi;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.BatchStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.repository.QianyiDetailRepository;
import com.example.moveprog.repository.QianyiRepository;
import com.example.moveprog.service.LoadService;
import com.example.moveprog.service.TranscodeService;
import com.example.moveprog.service.VerifyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class MigrationDispatcher {

    private final QianyiDetailRepository detailRepo;
    private final QianyiRepository qianyiRepo;
    private final TranscodeService transcodeService;
    private final LoadService loadService;
    private final VerifyService verifyService;

    @Scheduled(fixedDelay = 2000)
    public void dispatch() {
        // 查询待处理的任务 (根据你的业务逻辑)
        List<QianyiDetail> details = detailRepo.findByStatusIn(Arrays.asList(
                DetailStatus.NEW,
                DetailStatus.WAIT_LOAD, DetailStatus.WAIT_RELOAD,
                DetailStatus.WAIT_VERIFY
        ));

        for (QianyiDetail detail : details) {
            switch (detail.getStatus()) {
                case NEW:
                    transcodeService.execute(detail.getId());
                    break;
                case WAIT_LOAD:
                case WAIT_RELOAD:
                    loadService.execute(detail.getId());
                    break;
                case WAIT_VERIFY:
                    verifyService.execute(detail.getId());
                    break;
                default:
                    break;
            }
        }

        // 4. 批次完成检查 ...
        checkBatchCompletion();
    }

    private void checkBatchCompletion() {
        // 查找所有 PROCESSING 的主单
        List<Qianyi> runningBatches = qianyiRepo.findByStatus(BatchStatus.PROCESSING);
        for (Qianyi batch : runningBatches) {
            // 检查旗下是否还有未 PASS 的 Detail
            long notFinishedCount = detailRepo.countByQianyiIdAndStatusNot(batch.getId(), DetailStatus.PASS);
            if (notFinishedCount == 0) {
                batch.setStatus(BatchStatus.FINISHED);
                qianyiRepo.save(batch);
                log.info(">>> 批次完成: JobId={}, Table={}", batch.getId(), batch.getTableName());
            }
        }
    }
}