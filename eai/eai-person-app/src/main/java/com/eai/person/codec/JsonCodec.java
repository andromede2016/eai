package com.eai.person.codec;

import java.util.LinkedHashMap;
import java.util.Map;

import com.eai.framework.core.Message;
import com.eai.framework.spi.FormatCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * JSON codec (typical Kafka payload format).
 * decode: flat JSON object -> Map&lt;String,String&gt; (same shape as {@link XmlCodec}, so the same
 * validator/mapper work for both). encode: any object (e.g. Person) -> JSON.
 */
@RequiredArgsConstructor
public class JsonCodec implements FormatCodec {

    private final @NonNull ObjectMapper mapper;

    @Override public String format() { return "json"; }

    @Override
    public Object decode(byte[] raw, Message ctx) throws Exception {
        Map<String, Object> json = mapper.readValue(raw, new TypeReference<>() { });
        Map<String, String> flat = new LinkedHashMap<>();
        json.forEach((k, v) -> flat.put(k, v == null ? null : v.toString()));
        return flat;
    }

    @Override
    public byte[] encode(Object body, Message ctx) throws Exception {
        return mapper.writeValueAsBytes(body);
    }
}
