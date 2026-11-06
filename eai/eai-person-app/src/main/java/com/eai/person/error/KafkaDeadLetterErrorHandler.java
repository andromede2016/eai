package com.eai.person.error;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

import com.eai.framework.core.Exchange;
import com.eai.framework.core.Message;
import com.eai.framework.spi.ErrorHandler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka equivalent of the JMS DLQ: publishes the original payload to a "dead letter topic" (DLT),
 * with the reason as Kafka headers (eai.status, eai.errors, eai.failure) and the original coordinates
 * (eai.original.topic/partition/offset) so the record can be found and replayed.
 */
@Slf4j
@RequiredArgsConstructor
public class KafkaDeadLetterErrorHandler implements ErrorHandler {

    private final KafkaTemplate<String, byte[]> kafka;
    private final String deadLetterTopic;

    @Override
    public void handle(Exchange ex) {
        Message m = ex.getMessage();
        String errors = ex.getValidationErrors().stream()
            .map(e -> e.field() + ": " + e.message()).collect(Collectors.joining("; "));
        String failure = ex.getFailure() == null ? "" : ex.getFailure().toString();
        log.warn("Message {} {} {} {}", m.getId(), ex.getStatus(), errors, failure);
        if (deadLetterTopic == null || deadLetterTopic.isBlank()) return;

        String key = m.getHeaderAsString("kafka.key");
        ProducerRecord<String, byte[]> rec = new ProducerRecord<>(deadLetterTopic, key,
            m.getOriginalPayload() == null ? new byte[0] : m.getOriginalPayload());
        h(rec, "eai.status", ex.getStatus().name());
        h(rec, "eai.errors", errors);
        h(rec, "eai.failure", failure);
        h(rec, "eai.messageId", m.getId());
        h(rec, "eai.original.topic", m.getHeaderAsString("kafka.topic"));
        h(rec, "eai.original.partition", m.getHeaderAsString("kafka.partition"));
        h(rec, "eai.original.offset", m.getHeaderAsString("kafka.offset"));
        try {
            kafka.send(rec).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            // rethrow -> the Kafka container retries the record (not committed)
            throw new IllegalStateException("Cannot publish to DLT " + deadLetterTopic, e);
        }
    }

    private static void h(ProducerRecord<String, byte[]> rec, String k, String v) {
        if (v != null) rec.headers().add(k, v.getBytes(StandardCharsets.UTF_8));
    }
}
