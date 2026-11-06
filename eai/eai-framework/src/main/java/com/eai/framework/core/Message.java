package com.eai.framework.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

/**
 * Canonical message flowing through the kernel.
 * Raw payload (bytes) is set by inbound channels / encoders; body (domain object) by decoders/processors.
 */
@Getter
@Setter
@ToString(exclude = {"rawPayload", "originalPayload"})
public final class Message {

    private final String id = UUID.randomUUID().toString();
    @Getter(AccessLevel.NONE)
    private final Map<String, Object> headers;
    /** Payload as received, never modified (useful for dead-lettering / replay). */
    @Setter(AccessLevel.NONE)
    private final byte[] originalPayload;
    private byte[] rawPayload;
    private Object body;

    public Message(byte[] rawPayload, Map<String, Object> headers) {
        this.originalPayload = rawPayload;
        this.rawPayload = rawPayload;
        this.headers = new HashMap<>(headers == null ? Map.of() : headers);
    }

    @SuppressWarnings("unchecked")
    public <T> T getBody(Class<T> type) { return (T) body; }

    public Map<String, Object> getHeaders() { return Collections.unmodifiableMap(headers); }
    public Object getHeader(String name) { return headers.get(name); }
    public String getHeaderAsString(String name) { Object v = headers.get(name); return v == null ? null : v.toString(); }
    public Message setHeader(@NonNull String name, Object value) { headers.put(name, value); return this; }
}
