package com.eai.framework;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.eai.framework.core.Exchange;
import com.eai.framework.core.ExchangeStatus;
import com.eai.framework.core.Headers;
import com.eai.framework.core.Message;
import com.eai.framework.core.ValidationError;
import com.eai.framework.engine.IntegrationEngine;
import com.eai.framework.pipeline.Route;
import com.eai.framework.spi.OutboundChannel;
import com.eai.framework.support.CsvCodec;

class IntegrationEngineTest {

    @SuppressWarnings("unchecked")
    @Test
    void fullFlow() {
        List<String> sent = new ArrayList<>();
        OutboundChannel memory = new OutboundChannel() {
            public String type() { return "memory"; }
            public void send(Exchange ex, String dest) {
                sent.add(dest + ":" + new String(ex.getMessage().getRawPayload(), StandardCharsets.UTF_8));
            }
        };

        IntegrationEngine engine = new IntegrationEngine()
            .codec(new CsvCodec())
            .outbound(memory)
            .route(Route.builder("orders")
                .validate(ex -> ((List<?>) ex.getMessage().getBody()).isEmpty()
                        ? List.of(new ValidationError("body", "empty")) : List.of())
                .process(ex -> { /* e.g. DB insert, send mail */ })
                .enrich(ex -> ((List<Map<String, String>>) ex.getMessage().getBody())
                        .forEach(r -> r.put("status", "OK")))
                .build());

        Exchange ex = engine.dispatch(new Message("id;name\n1;foo\n".getBytes(StandardCharsets.UTF_8), Map.of(
            Headers.INPUT_FORMAT, "csv",
            Headers.OUTPUT_FORMAT, "csv",
            Headers.OUTPUT_CHANNEL, "memory",
            Headers.OUTPUT_DESTINATION, "Q1")));

        assertEquals(ExchangeStatus.SENT, ex.getStatus());
        assertEquals("Q1:id;name;status\n1;foo;OK\n", sent.get(0));

        Exchange rejected = engine.dispatch(new Message("id;name\n".getBytes(StandardCharsets.UTF_8),
            Map.of(Headers.INPUT_FORMAT, "csv")));
        assertEquals(ExchangeStatus.REJECTED, rejected.getStatus());
    }
}
