package com.example.moveprog.util;

import org.apache.commons.lang3.tuple.Pair;

import java.nio.charset.CharsetEncoder;
import java.util.Objects;

public class FastEscapeHandler {

    // 定义状态常量
    private static final int STATE_NORMAL = 0;       // 正常读取模式
    private static final int STATE_IN_ESCAPE = 1;    // 遇到反斜杠，进入转义模式

    /**
     * 【核心】状态机解析：将含转义符的 ASCII 串还原为 Unicode
     * 逻辑：
     * 1. 遇到 '\' 进入转义收集模式
     * 2. 在收集模式下遇到另一个 '\'，则结束收集，将缓冲区解析为 Hex -> Char
     * 3. 支持转义符本身的转义：即 "\\" 会被解析为 "\"
     */
    public static Pair<Boolean,String> unescape(String input) {
        if (input == null) return Pair.of(false,null);
        // 快速路径：如果不含转义符，直接返回原串，零拷贝
        if (input.indexOf('\\') == -1) {
            return Pair.of(false,input);
        }

        StringBuilder out = new StringBuilder(input.length());
        StringBuilder hexBuffer = new StringBuilder(16); // 用于暂存 Hex 码
        int state = STATE_NORMAL;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            switch (state) {
                case STATE_NORMAL:
                    if (c == '\\') {
                        state = STATE_IN_ESCAPE; // 切换状态
                    } else {
                        out.append(c); // 正常字符直接输出
                    }
                    break;

                case STATE_IN_ESCAPE:
                    if (c == '\\') {
                        // 遇到闭合的 '\'
                        if (hexBuffer.length() == 0) {
                            // 缓冲区为空，说明是 "\\" -> 转义成了 "\"
                            out.append('\\');
                        } else {
                            // 缓冲区有值，尝试解析 Hex
                            try {
                                int codePoint = Integer.parseInt(hexBuffer.toString(), 16);
                                out.append(Character.toChars(codePoint));
                            } catch (NumberFormatException e) {
                                // 容错：如果里边不是 Hex，说明不是合法转义，原样还原
                                // 比如原本是 "C:\Windows\"，被误判了
                                out.append('\\').append(hexBuffer).append('\\');
                            }
                        }
                        // 清空缓冲，切回正常模式
                        hexBuffer.setLength(0);
                        state = STATE_NORMAL;
                    } else {
                        // 还在转义符中间，收集 Hex 字符
                        hexBuffer.append(c);
                    }
                    break;
            }
        }

        // 边缘情况处理：如果字符串以 '\' 结尾（断头转义）
        // 比如 "abc\123" 后面没有闭合的斜杠
        if (state == STATE_IN_ESCAPE) {
            out.append('\\').append(hexBuffer);
        }

        String processed = out.toString();
        return Pair.of(Objects.equals(input,processed), processed);
    }

    /**
     * 【方案B专用】将 Unicode 串重新编码为转义格式
     * 用于验证：input -> 尝试编码 -> 失败则生成 \HEX\
     */
    public static String escapeForCheck(String input, CharsetEncoder encoder) {
        if (input == null) return null;
        
        StringBuilder sb = new StringBuilder(input.length() + 16);
        
        for (int i = 0; i < input.length(); ) {
            int cp = input.codePointAt(i);
            int charCount = Character.charCount(cp);
            
            // 截取当前这一个字（可能是单字符，也可能是双字符的 Emoji/生僻字）
            CharSequence currentChar = input.subSequence(i, i + charCount);

            // 核心判断：IBM1388 能不能直接存这个字？
            if (encoder.canEncode(currentChar)) {
                // 能存：比如 "张"，直接保留
                sb.append(currentChar);
            } else {
                // 不能存：比如 "𬱖" -> 生成转义序列 \2CC56\
                // 注意：如果字符本身是 '\'，也在这里被处理成 "\\" (因为 canEncode('\\') 是 true，这行逻辑要在下面细分)
                
                // 特殊处理：如果字符本身就是 '\' (Backslash)
                if (cp == '\\') {
                    sb.append("\\\\"); 
                } else {
                    sb.append('\\').append(String.format("%X", cp)).append('\\');
                }
            }
            i += charCount;
        }
        return sb.toString();
    }
}