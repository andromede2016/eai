package com.eai.person.channel;

import java.util.Map;

import com.eai.framework.core.Headers;

import lombok.experimental.UtilityClass;

/**
 * Detects the input format from the first non-blank character of the payload
 * ('{' or '[' -> json, '<' -> xml). An explicit {@code eai.input.format} sent with the message wins;
 * the channel's configured format is only used when the content is not recognised.
 */
@UtilityClass
public class FormatSniffer {

    public void apply(byte[] payload, Map<String, Object> headers, boolean explicit) {
        if (explicit || payload == null) return;
        for (byte b : payload) {
            if (Character.isWhitespace(b) || b == (byte) 0xEF || b == (byte) 0xBB || b == (byte) 0xBF) continue; // skip blanks + UTF-8 BOM
            if (b == '{' || b == '[') headers.put(Headers.INPUT_FORMAT, "json");
            else if (b == '<') headers.put(Headers.INPUT_FORMAT, "xml");
            return;
        }
    }
}
