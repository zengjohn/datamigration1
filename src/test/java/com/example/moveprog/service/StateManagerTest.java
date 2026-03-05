package com.example.moveprog.service;

import com.example.moveprog.entity.CsvSplit;
import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.entity.QianyiDetail;
import com.example.moveprog.enums.CsvSplitStatus;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.enums.JobStatus;
import com.example.moveprog.repository.CsvSplitRepository;
import com.example.moveprog.repository.MigrationJobRepository;
import com.example.moveprog.repository.QianyiDetailRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * StateManager 单元测试
 * 测试状态机转换逻辑
 */
@ExtendWith(MockitoExtension.class)
class StateManagerTest {

    @Mock
    private CsvSplitRepository splitRepo;
    @Mock
    private QianyiDetailRepository detailRepo;
    @Mock
    private MigrationJobRepository jobRepo;

    @InjectMocks
    private StateManager stateManager;

    private MigrationJob testJob;
    private CsvSplit testSplit;
    private QianyiDetail testDetail;

    @BeforeEach
    void setUp() {
        // 创建测试数据
        testJob = new MigrationJob();
        testJob.setId(1L);
        testJob.setStatus(JobStatus.ACTIVE);
        testJob.setName("测试作业");

        testSplit = new CsvSplit();
        testSplit.setId(100L);
        testSplit.setJobId(1L);
        testSplit.setDetailId(10L);

        testDetail = new QianyiDetail();
        testDetail.setId(10L);
    }

    // ========================
    // 测试1: 合法状态转换
    // ========================

    @Test
    @DisplayName("测试合法状态转换: WAIT_LOAD -> LOADING")
    void testSwitchStatus_WaitLoadToLoading() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.WAIT_LOAD);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(testJob));
        when(splitRepo.save(any(CsvSplit.class))).thenReturn(testSplit);

        // Act
        boolean result = stateManager.switchSplitStatus(100L, CsvSplitStatus.LOADING, "开始装载");

        // Assert
        assertTrue(result);
        assertEquals(CsvSplitStatus.LOADING, testSplit.getStatus());
        verify(splitRepo).save(testSplit);
    }

    @Test
    @DisplayName("测试合法状态转换: LOADING -> WAIT_VERIFY")
    void testSwitchStatus_LoadingToWaitVerify() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.LOADING);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(testJob));
        when(splitRepo.save(any(CsvSplit.class))).thenReturn(testSplit);

        // Act
        boolean result = stateManager.switchSplitStatus(100L, CsvSplitStatus.WAIT_VERIFY, "装载完成");

        // Assert
        assertTrue(result);
        assertEquals(CsvSplitStatus.WAIT_VERIFY, testSplit.getStatus());
    }

    @Test
    @DisplayName("测试合法状态转换: WAIT_VERIFY -> VERIFYING")
    void testSwitchStatus_WaitVerifyToVerifying() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.WAIT_VERIFY);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(testJob));
        when(splitRepo.save(any(CsvSplit.class))).thenReturn(testSplit);

        // Act
        boolean result = stateManager.switchSplitStatus(100L, CsvSplitStatus.VERIFYING, "开始验证");

        // Assert
        assertTrue(result);
        assertEquals(CsvSplitStatus.VERIFYING, testSplit.getStatus());
    }

    @Test
    @DisplayName("测试合法状态转换: VERIFYING -> PASS")
    void testSwitchStatus_VerifyingToPass() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.VERIFYING);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(testJob));
        when(splitRepo.save(any(CsvSplit.class))).thenReturn(testSplit);

        // Act
        boolean result = stateManager.switchSplitStatus(100L, CsvSplitStatus.PASS, "校验通过");

        // Assert
        assertTrue(result);
        assertEquals(CsvSplitStatus.PASS, testSplit.getStatus());
    }

    @Test
    @DisplayName("测试合法状态转换: VERIFYING -> FAIL_VERIFY")
    void testSwitchStatus_VerifyingToFailVerify() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.VERIFYING);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(testJob));
        when(splitRepo.save(any(CsvSplit.class))).thenReturn(testSplit);

        // Act
        boolean result = stateManager.switchSplitStatus(100L, CsvSplitStatus.FAIL_VERIFY, "发现差异");

        // Assert
        assertTrue(result);
        assertEquals(CsvSplitStatus.FAIL_VERIFY, testSplit.getStatus());
        assertEquals("发现差异", testSplit.getErrorMsg());
    }

    @Test
    @DisplayName("测试合法状态转换: FAIL_LOAD -> WAIT_LOAD (重试)")
    void testSwitchStatus_FailLoadToWaitLoad_Retry() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.FAIL_LOAD);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(testJob));
        when(splitRepo.save(any(CsvSplit.class))).thenReturn(testSplit);

        // Act
        boolean result = stateManager.switchSplitStatus(100L, CsvSplitStatus.WAIT_LOAD, "人工重试");

        // Assert
        assertTrue(result);
        assertEquals(CsvSplitStatus.WAIT_LOAD, testSplit.getStatus());
    }

    // ========================
    // 测试2: 非法状态转换
    // ========================

    @Test
    @DisplayName("测试非法状态转换: PASS -> LOADING (应拒绝)")
    void testSwitchStatus_Illegal_PassToLoading() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.PASS);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(testJob));

        // Act
        boolean result = stateManager.switchSplitStatus(100L, CsvSplitStatus.LOADING, "尝试回退");

        // Assert
        assertFalse(result);
        assertEquals(CsvSplitStatus.PASS, testSplit.getStatus()); // 状态不应改变
        verify(splitRepo, never()).save(any()); // 不应保存
    }

    @Test
    @DisplayName("测试非法状态转换: WAIT_LOAD -> PASS (应拒绝)")
    void testSwitchStatus_Illegal_WaitLoadToPass() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.WAIT_LOAD);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(testJob));

        // Act
        boolean result = stateManager.switchSplitStatus(100L, CsvSplitStatus.PASS, "跳跃");

        // Assert
        assertFalse(result);
        assertEquals(CsvSplitStatus.WAIT_LOAD, testSplit.getStatus());
    }

    @Test
    @DisplayName("测试非法状态转换: WAIT_LOAD -> VERIFYING (应拒绝)")
    void testSwitchStatus_Illegal_WaitLoadToVerifying() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.WAIT_LOAD);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(testJob));

        // Act
        boolean result = stateManager.switchSplitStatus(100L, CsvSplitStatus.VERIFYING, "跳跃");

        // Assert
        assertFalse(result);
        assertEquals(CsvSplitStatus.WAIT_LOAD, testSplit.getStatus());
    }

    // ========================
    // 测试3: Job停止时拒绝更新
    // ========================

    @Test
    @DisplayName("测试Job停止时拒绝Split状态更新: STOPPED")
    void testSwitchStatus_JobStopped() {
        // Arrange
        testJob.setStatus(JobStatus.STOPPED);
        testSplit.setStatus(CsvSplitStatus.WAIT_LOAD);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(testJob));

        // Act
        boolean result = stateManager.switchSplitStatus(100L, CsvSplitStatus.LOADING, "尝试装载");

        // Assert
        assertFalse(result);
        assertEquals(CsvSplitStatus.WAIT_LOAD, testSplit.getStatus()); // 状态不应改变
    }

    @Test
    @DisplayName("测试Job停止时拒绝Split状态更新: PAUSED")
    void testSwitchStatus_JobPaused() {
        // Arrange
        testJob.setStatus(JobStatus.PAUSED);
        testSplit.setStatus(CsvSplitStatus.WAIT_LOAD);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(jobRepo.findById(1L)).thenReturn(Optional.of(testJob));

        // Act
        boolean result = stateManager.switchSplitStatus(100L, CsvSplitStatus.LOADING, "尝试装载");

        // Assert
        assertFalse(result);
        assertEquals(CsvSplitStatus.WAIT_LOAD, testSplit.getStatus());
    }

    // ========================
    // 测试4: resetSplitForRetry
    // ========================

    @Test
    @DisplayName("测试重置FAIL_LOAD状态")
    void testResetSplitForRetry_FailLoad() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.FAIL_LOAD);
        testSplit.setErrorMsg("原错误信息");
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(splitRepo.save(any(CsvSplit.class))).thenReturn(testSplit);

        // Act
        stateManager.resetSplitForRetry(100L);

        // Assert
        assertEquals(CsvSplitStatus.WAIT_LOAD, testSplit.getStatus());
        assertEquals("人工重试-等待装载", testSplit.getErrorMsg());
    }

    @Test
    @DisplayName("测试重置FAIL_VERIFY状态")
    void testResetSplitForRetry_FailVerify() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.FAIL_VERIFY);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));
        when(splitRepo.save(any(CsvSplit.class))).thenReturn(testSplit);

        // Act
        stateManager.resetSplitForRetry(100L);

        // Assert
        assertEquals(CsvSplitStatus.WAIT_LOAD, testSplit.getStatus());
    }

    @Test
    @DisplayName("测试非失败状态不允许重试")
    void testResetSplitForRetry_IllegalState() {
        // Arrange
        testSplit.setStatus(CsvSplitStatus.PASS);
        when(splitRepo.findById(100L)).thenReturn(Optional.of(testSplit));

        // Act & Assert
        assertThrows(RuntimeException.class, () -> {
            stateManager.resetSplitForRetry(100L);
        });
    }

    // ========================
    // 测试5: refreshDetailStatus
    // ========================

    @Test
    @DisplayName("测试刷新Detail状态: 全部PASS -> FINISHED")
    void testRefreshDetailStatus_AllPass() {
        // Arrange
        testDetail.setStatus(DetailStatus.PROCESSING_CHILDS);
        
        CsvSplit split1 = new CsvSplit();
        split1.setStatus(CsvSplitStatus.PASS);
        
        CsvSplit split2 = new CsvSplit();
        split2.setStatus(CsvSplitStatus.PASS);

        when(detailRepo.findById(10L)).thenReturn(Optional.of(testDetail));
        when(splitRepo.findByDetailId(10L)).thenReturn(List.of(split1, split2));
        when(detailRepo.save(any())).thenReturn(testDetail);

        // Act
        stateManager.refreshDetailStatus(10L);

        // Assert
        assertEquals(DetailStatus.FINISHED, testDetail.getStatus());
    }

    @Test
    @DisplayName("测试刷新Detail状态: 有Running -> PROCESSING_CHILDS")
    void testRefreshDetailStatus_HasRunning() {
        // Arrange
        testDetail.setStatus(DetailStatus.NEW);
        
        CsvSplit split1 = new CsvSplit();
        split1.setStatus(CsvSplitStatus.PASS);
        
        CsvSplit split2 = new CsvSplit();
        split2.setStatus(CsvSplitStatus.LOADING); // 运行中

        when(detailRepo.findById(10L)).thenReturn(Optional.of(testDetail));
        when(splitRepo.findByDetailId(10L)).thenReturn(List.of(split1, split2));
        when(detailRepo.save(any())).thenReturn(testDetail);

        // Act
        stateManager.refreshDetailStatus(10L);

        // Assert
        assertEquals(DetailStatus.PROCESSING_CHILDS, testDetail.getStatus());
    }

    @Test
    @DisplayName("测试刷新Detail状态: 有失败 -> FINISHED_WITH_ERROR")
    void testRefreshDetailStatus_HasFail() {
        // Arrange
        testDetail.setStatus(DetailStatus.PROCESSING_CHILDS);
        
        CsvSplit split1 = new CsvSplit();
        split1.setStatus(CsvSplitStatus.PASS);
        
        CsvSplit split2 = new CsvSplit();
        split2.setStatus(CsvSplitStatus.FAIL_LOAD); // 失败

        when(detailRepo.findById(10L)).thenReturn(Optional.of(testDetail));
        when(splitRepo.findByDetailId(10L)).thenReturn(List.of(split1, split2));
        when(detailRepo.save(any())).thenReturn(testDetail);

        // Act
        stateManager.refreshDetailStatus(10L);

        // Assert
        assertEquals(DetailStatus.FINISHED_WITH_ERROR, testDetail.getStatus());
    }

    @Test
    @DisplayName("测试刷新Detail状态: 无Splits时不更新")
    void testRefreshDetailStatus_NoSplits() {
        // Arrange
        testDetail.setStatus(DetailStatus.NEW);
        when(detailRepo.findById(10L)).thenReturn(Optional.of(testDetail));
        when(splitRepo.findByDetailId(10L)).thenReturn(List.of());

        // Act
        stateManager.refreshDetailStatus(10L);

        // Assert
        assertEquals(DetailStatus.NEW, testDetail.getStatus()); // 保持不变
        verify(detailRepo, never()).save(any()); // 不应保存
    }
}