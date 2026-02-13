package com.example.moveprog.util;

import com.ibm.icu.charset.CharsetICU;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

@Slf4j
public class CharsetFactory {

    public static CharsetDecoder createDecoder(Charset charset, boolean strict) {
        // JDK 21 依然沿用这一套标准的 Decoder 逻辑
        CharsetDecoder decoder = charset.newDecoder();
        
        if (strict) {
            // 严格模式：遇到乱码直接抛异常 (MalformedInputException)
            decoder.onMalformedInput(CodingErrorAction.REPORT);
            decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        } else {
            // 宽松模式：替换为无意义字符
            decoder.onMalformedInput(CodingErrorAction.REPLACE);
            decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
            // 替换字符通常是  (U+FFFD) 或 ?
            decoder.replaceWith("\uFFFD");
        }
        
        return decoder;
    }

    /**
     * 尝试加载字符集，优先 JDK 原生，降级使用 ICU
     */
    public static Charset resolveCharset(String name) {
        // 1. 尝试标准 JDK 加载 (JDK 21 通常支持 x-IBM1388)
        try {
            return Charset.forName(name);
        } catch (Exception e) {
            log.debug("JDK原生不支持 {}, 尝试使用 ICU4J...", name);
        }

        // 2. 尝试 ICU4J 加载
        try {
            return CharsetICU.forNameICU(name);
        } catch (Exception e) {
            log.error("致命错误: 无法识别的字符集编码 [{}]", name);
            throw new RuntimeException("不支持的字符集: " + name, e);
        }
    }
}