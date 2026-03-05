package com.example.moveprog.service;

import com.example.moveprog.service.impl.CsvRowIterator;
import com.example.moveprog.service.impl.JdbcRowIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;


import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * CoreComparator 单元测试
 * 测试核心行级比对逻辑
 */
@ExtendWith(MockitoExtension.class)
class CoreComparatorTest {

    @Spy
    private JobControlManager jobControlManager;

    @Mock
    private CsvRowIterator fileIterator;

    @Mock
    private JdbcRowIterator dbIterator;

    @Mock
    private VerifyDiffWriter diffWriter;

    @InjectMocks
    private CoreComparator coreComparator;

    // ========================
    // 测试1: 数据完全匹配
    // ========================

    @Test
    @DisplayName("测试完全匹配场景：无差异")
    void testCompareStreams_AllMatch() throws Exception {

        // CSV数据: 行号1,2,3
        when(fileIterator.hasNext()).thenReturn(true, true, true, false);
        when(fileIterator.next())
            .thenReturn(new String[]{"AAA", "100", "1"})
            .thenReturn(new String[]{"BBB", "200", "2"})
            .thenReturn(new String[]{"CCC", "300", "3"});

        // DB数据: 行号1,2,3
        when(dbIterator.hasNext()).thenReturn(true, true, true, false);
        when(dbIterator.next())
            .thenReturn(new Object[]{"AAA", "100", 1L})
            .thenReturn(new Object[]{"BBB", "200", 2L})
            .thenReturn(new Object[]{"CCC", "300", 3L});
        
        when(dbIterator.getColumnNames()).thenReturn(new String[]{"col1", "col2", "row_no"});
        when(dbIterator.getColumnTypes()).thenReturn(new int[]{java.sql.Types.VARCHAR, java.sql.Types.VARCHAR, java.sql.Types.BIGINT});

        AtomicLong atomicLong = new AtomicLong(0);
        when(diffWriter.getDiffCount()).thenReturn(atomicLong);

        // Act
        long diffCount = coreComparator.compareStreams(1L, fileIterator, dbIterator, diffWriter);

        // Assert
        assertEquals(0, diffCount);
        verify(diffWriter, never()).writeDiff(anyString());
    }

    // ========================
    // 测试2: 行号不匹配
    // ========================

    @Test
    @DisplayName("测试行号不匹配: CSV有行号5，DB有行号10")
    void testCompareStreams_RowNoMismatch_FileHasMore() throws Exception {
        // CSV: 行号 1, 2, 3, 5
        when(fileIterator.hasNext()).thenReturn(true, true, true, true, false);
        when(fileIterator.next())
            .thenReturn(new String[]{"AAA", "100", "1"})
            .thenReturn(new String[]{"BBB", "200", "2"})
            .thenReturn(new String[]{"CCC", "300", "3"})
            .thenReturn(new String[]{"EEE", "500", "5"});

        // DB: 行号 1, 2, 3, 10
        when(dbIterator.hasNext()).thenReturn(true, true, true, true, false);
        when(dbIterator.next())
            .thenReturn(new Object[]{"AAA", "100", 1L})
            .thenReturn(new Object[]{"BBB", "200", 2L})
            .thenReturn(new Object[]{"CCC", "300", 3L})
            .thenReturn(new Object[]{"DDD", "400", 10L});

        when(dbIterator.getColumnNames()).thenReturn(new String[]{"col1", "col2", "row_no"});
        when(dbIterator.getColumnTypes()).thenReturn(new int[]{java.sql.Types.VARCHAR, java.sql.Types.VARCHAR, java.sql.Types.BIGINT});

        AtomicLong atomicLong = new AtomicLong(0);
        when(diffWriter.getDiffCount()).thenReturn(atomicLong);

        // Act
        long diffCount = coreComparator.compareStreams(1L, fileIterator, dbIterator, diffWriter);

        // Assert
        assertEquals(2, diffCount);
        verify(diffWriter).writeDiff(contains("!{5}"));
        verify(diffWriter).writeDiff(contains("!{10}"));
    }

    // ========================
    // 测试3: 内容不一致
    // ========================

    @Test
    @DisplayName("测试内容不一致: 正确识别差异列")
    void testCompareStreams_ContentMismatch() throws Exception {
        // CSV: 行1
        when(fileIterator.hasNext()).thenReturn(true, false);
        when(fileIterator.next()).thenReturn(new String[]{"AAA", "100", "1"});

        // DB: 相同行号但内容不同
        when(dbIterator.hasNext()).thenReturn(true, false);
        when(dbIterator.next()).thenReturn(new Object[]{"AAA", "999", 1L});

        when(dbIterator.getColumnNames()).thenReturn(new String[]{"col1", "col2", "row_no"});
        when(dbIterator.getColumnTypes()).thenReturn(new int[]{java.sql.Types.VARCHAR, java.sql.Types.VARCHAR, java.sql.Types.BIGINT});

        when(diffWriter.getDiffCount()).thenReturn(new java.util.concurrent.atomic.AtomicLong(0));

        // Act
        long diffCount = coreComparator.compareStreams(1L, fileIterator, dbIterator, diffWriter);

        // Assert
        assertEquals(1, diffCount);
        verify(diffWriter).writeDiff(anyString());
    }

    // ========================
    // 测试4: 空数据场景
    // ========================

    @Test
    @DisplayName("测试空文件场景")
    void testCompareStreams_EmptyFile() throws Exception {
        // CSV: 无数据
        when(fileIterator.hasNext()).thenReturn(false);
        
        // DB: 有数据
        when(dbIterator.hasNext()).thenReturn(true, false);
        when(dbIterator.next()).thenReturn(new Object[]{"AAA", "100", 1L});

        when(dbIterator.getColumnNames()).thenReturn(new String[]{"col1", "col2", "row_no"});
        when(dbIterator.getColumnTypes()).thenReturn(new int[]{java.sql.Types.VARCHAR, java.sql.Types.VARCHAR, java.sql.Types.BIGINT});

        when(diffWriter.getDiffCount()).thenReturn(new java.util.concurrent.atomic.AtomicLong(0));

        // Act
        long diffCount = coreComparator.compareStreams(1L, fileIterator, dbIterator, diffWriter);

        // Assert
        assertEquals(1, diffCount);
        verify(diffWriter).writeDiff(contains("CSV: null"));
    }

    @Test
    @DisplayName("测试空数据库场景")
    void testCompareStreams_EmptyDb() throws Exception {
        // CSV: 有数据
        when(fileIterator.hasNext()).thenReturn(true, false);
        when(fileIterator.next()).thenReturn(new String[]{"AAA", "100", "1"});

        // DB: 无数据
        when(dbIterator.hasNext()).thenReturn(false);

        when(dbIterator.getColumnNames()).thenReturn(new String[]{"col1", "col2", "row_no"});
        when(dbIterator.getColumnTypes()).thenReturn(new int[]{java.sql.Types.VARCHAR, java.sql.Types.VARCHAR, java.sql.Types.BIGINT});

        when(diffWriter.getDiffCount()).thenReturn(new java.util.concurrent.atomic.AtomicLong(0));

        // Act
        long diffCount = coreComparator.compareStreams(1L, fileIterator, dbIterator, diffWriter);

        // Assert
        assertEquals(1, diffCount);
        verify(diffWriter).writeDiff(contains("DB: null"));
    }

    // ========================
    // 测试5: NULL值处理
    // ========================

    @Test
    @DisplayName("测试NULL vs NULL: 应该相等")
    void testCompareStreams_NullEqualsNull() throws Exception {
        // CSV: NULL
        when(fileIterator.hasNext()).thenReturn(true, false);
        when(fileIterator.next()).thenReturn(new String[]{null, "100", "1"});

        // DB: NULL
        when(dbIterator.hasNext()).thenReturn(true, false);
        when(dbIterator.next()).thenReturn(new Object[]{null, "100", 1L});

        when(dbIterator.getColumnNames()).thenReturn(new String[]{"col1", "col2", "row_no"});
        when(dbIterator.getColumnTypes()).thenReturn(new int[]{java.sql.Types.VARCHAR, java.sql.Types.VARCHAR, java.sql.Types.BIGINT});

        when(diffWriter.getDiffCount()).thenReturn(new java.util.concurrent.atomic.AtomicLong(0));

        // Act
        long diffCount = coreComparator.compareStreams(1L, fileIterator, dbIterator, diffWriter);

        // Assert
        assertEquals(0, diffCount);
        verify(diffWriter, never()).writeDiff(anyString());
    }

    @Test
    @DisplayName("测试NULL vs 有值: 应该不等")
    void testCompareStreams_NullVsValue() throws Exception {
        // CSV: NULL
        when(fileIterator.hasNext()).thenReturn(true, false);
        when(fileIterator.next()).thenReturn(new String[]{null, "100", "1"});

        // DB: 有值
        when(dbIterator.hasNext()).thenReturn(true, false);
        when(dbIterator.next()).thenReturn(new Object[]{"AAA", "100", 1L});

        when(dbIterator.getColumnNames()).thenReturn(new String[]{"col1", "col2", "row_no"});
        when(dbIterator.getColumnTypes()).thenReturn(new int[]{java.sql.Types.VARCHAR, java.sql.Types.VARCHAR, java.sql.Types.BIGINT});

        when(diffWriter.getDiffCount()).thenReturn(new java.util.concurrent.atomic.AtomicLong(0));

        // Act
        long diffCount = coreComparator.compareStreams(1L, fileIterator, dbIterator, diffWriter);

        // Assert
        assertEquals(1, diffCount);
        verify(diffWriter).writeDiff(anyString());
    }

    // ========================
    // 测试6: 数字类型精度比较
    // ========================

    @Test
    @DisplayName("测试数字精度: 1.0 == 1.00")
    void testCompareStreams_NumberPrecision() throws Exception {
        // CSV: 1.0
        when(fileIterator.hasNext()).thenReturn(true, false);
        when(fileIterator.next()).thenReturn(new String[]{"1.0", "1"});

        // DB: 1.00
        when(dbIterator.hasNext()).thenReturn(true, false);
        when(dbIterator.next()).thenReturn(new Object[]{"1.00", 1L});

        when(dbIterator.getColumnNames()).thenReturn(new String[]{"col1", "row_no"});
        when(dbIterator.getColumnTypes()).thenReturn(new int[]{java.sql.Types.DECIMAL, java.sql.Types.BIGINT});

        when(diffWriter.getDiffCount()).thenReturn(new java.util.concurrent.atomic.AtomicLong(0));

        // Act
        long diffCount = coreComparator.compareStreams(1L, fileIterator, dbIterator, diffWriter);

        // Assert
        assertEquals(0, diffCount);
    }
}
