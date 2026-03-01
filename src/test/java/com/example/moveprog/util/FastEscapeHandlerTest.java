package com.example.moveprog.util;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FastEscapeHandlerTest {
    private static final String CHARSET_IBM = "x-IBM1388";

    @Test
    @DisplayName("只有转义unicode字符(单字)")
    void unescape_singleChar() {
        Pair<Boolean, String> unescape = FastEscapeHandler.unescape("\\00006724\\");
        assertTrue(unescape.getLeft());
        assertEquals("朤", unescape.getRight() );
    }

    @Test
    @DisplayName("只有转义unicode字符(多字)")
    void unescape_multiChars() {
        Pair<Boolean, String> unescape = FastEscapeHandler.unescape("\\00006724\\\\6724\\\\00009F98\\");
        assertTrue(unescape.getLeft());
        assertEquals("朤朤龘", unescape.getRight());
    }

    @Test
    @DisplayName("转义和不转义字符并存")
    void unescape_mixMultiChars() {
        Pair<Boolean, String> unescape = FastEscapeHandler.unescape("没有\\转义\\没有\\6724\\转义");
        assertTrue(unescape.getLeft());
        assertEquals("没有\\转义\\没有朤转义", unescape.getRight());
    }

    @Test
    @DisplayName("不转义字符")
    void unescape_normalChars() {
        Pair<Boolean, String> unescape = FastEscapeHandler.unescape("正常字符没有转义\\转义");
        assertFalse(unescape.getLeft());
        assertEquals("正常字符没有转义\\转义", unescape.getRight());
    }

    @Test
    @DisplayName("反斜杠")
    void unescape_backslash() {
        String original = "C:\\Windows\\_1";
        Pair<Boolean, String> unescape = FastEscapeHandler.unescape(original);
        assertFalse(unescape.getLeft());
        assertEquals(original, unescape.getRight());
    }

    @Test
    @DisplayName("2个反斜杠")
    void unescape_doublebackslash() {
        String original = "C:\\\\Windows\\_1";
        Pair<Boolean, String> unescape = FastEscapeHandler.unescape(original);
        assertFalse(unescape.getLeft());
        assertEquals(original, unescape.getRight());
    }


    @Test
    @DisplayName("Bug修复验证：反斜杠的稳定性检查")
    void escapeForCheck_backslash() {
        // 模拟一个总是返回 true 的 Encoder (模拟 IBM1388 支持反斜杠)
        // 使用 US_ASCII，它支持反斜杠
        java.nio.charset.CharsetEncoder encoder = java.nio.charset.StandardCharsets.US_ASCII.newEncoder();

        // 场景：原始串包含反斜杠（如 Windows 路径）
        String original = "C:\\Windows\\System32";

        // 1. 编码
        // 修复前：因为 encoder 支持 \，所以原样返回 "C:\Windows\System32"
        // 修复后：应该强制转义为 "C:\\Windows\\System32"
        String escaped = FastEscapeHandler.escapeForCheck(original, encoder);

        System.out.println("Original: " + original);
        System.out.println("Escaped : " + escaped);

        // 2. 解码 (模拟 checkCellStability 的过程)
        // unescape 会把 \\ 变成 \
        Pair<Boolean, String> result = FastEscapeHandler.unescape(escaped);

        // 3. 验证是否还原
        // 如果修复前，unescape("C:\Windows\System32") 会把 \W 和 \S 试图当做转义处理，导致还原失败
        assertEquals(original, result.getRight(), "反斜杠经过 escape -> unescape 后应该保持原样");
    }

}
