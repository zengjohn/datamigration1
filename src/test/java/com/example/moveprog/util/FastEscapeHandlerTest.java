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

}
