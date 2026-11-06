package com.eai.person.channel;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;

import com.eai.framework.core.Exchange;
import com.eai.framework.core.Headers;
import com.eai.framework.core.Message;
import com.eai.framework.spi.OutboundChannel;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Target "kafka": publishes the encoded payload to the topic given as destination.
 *
 * <p>The record <b>key</b> decides the partition (hash(key) % partitions): all records with the same key
 * land in the same partition and are therefore consumed <b>in order</b>. Here the key is the person id.
 * The send is awaited (synchronous) so that a broker failure surfaces as an exception and goes to the
 * route's error handler instead of being lost.
 */
@Slf4j
@RequiredArgsConstructor
public class KafkaOutboundChannel implements OutboundChannel {

    private final @NonNull KafkaTemplate<String, byte[]> kafka;
    private final @NonNull String defaultTopic;
    private final @NonNull Function<Exchange, String> keyExtractor;
    private final @NonNull ObjectMapper mapper;

    @Override public String type() { return "kafka"; }
    @Override public String defaultFormat() { return "json"; }

    @Override
    public void send(Exchange ex, String destination) throws Exception {
        String topic = destination == null ? defaultTopic : destination;
        Message m = ex.getMessage();
        byte[] value = m.getRawPayload();
        String format = m.getHeaderAsString(Headers.OUTPUT_FORMAT);
        // Format "object" (route default for the DB) produces no bytes: Kafka needs a serialized value -> JSON.
        if ((value == null || value.length == 0) && m.getBody() != null) {
            value = mapper.writeValueAsBytes(m.getBody());
            format = "json";
        }
        ProducerRecord<String, byte[]> rec = new ProducerRecord<>(topic, keyExtractor.apply(ex), value);
        header(rec, "eai.messageId", m.getId());
        header(rec, Headers.SOURCE_CHANNEL, m.getHeaderAsString(Headers.SOURCE_CHANNEL));
        header(rec, "content-format", format);
        RecordMetadata md = kafka.send(rec).get(10, TimeUnit.SECONDS).getRecordMetadata();
        log.info("[{}] published to {}-{}@{} key={}", m.getId(), md.topic(), md.partition(), md.offset(), rec.key());
    }

    private static void header(ProducerRecord<String, byte[]> rec, String k, String v) {
        if (v != null) rec.headers().add(k, v.getBytes(StandardCharsets.UTF_8));
    }
}
