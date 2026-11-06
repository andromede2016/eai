package com.eai.person.channel;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

import com.eai.framework.core.Headers;
import com.eai.framework.core.Message;
import com.eai.framework.spi.InboundChannel;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka input channel backed by Spring's {@link ConcurrentMessageListenerContainer}.
 *
 * <p>Kafka concepts illustrated here:
 * <ul>
 *   <li><b>topic / partition / offset</b>: a record is identified by (topic, partition, offset);
 *       they are copied to headers {@code kafka.topic}, {@code kafka.partition}, {@code kafka.offset}.</li>
 *   <li><b>consumer group</b> ({@code groupId}): all instances sharing the same group split the partitions
 *       between them (load balancing). A different group gets its own copy of every record (pub/sub).</li>
 *   <li><b>concurrency</b>: number of consumer threads; useful only up to the number of partitions.</li>
 *   <li><b>offset commit</b> (AckMode.RECORD): the offset is committed after each record is processed.
 *       If the listener throws, the record is retried by the container's error handler (at-least-once).</li>
 * </ul>
 * Kafka headers are byte[]; they are converted to UTF-8 strings. Unlike JMS, names may contain dots,
 * so {@code eai.output.channel} can be sent as-is.
 */
@Slf4j
@RequiredArgsConstructor
public class KafkaInboundChannel implements InboundChannel {

    @Getter @Accessors(fluent = true)
    private final @NonNull String name;
    private final @NonNull ConsumerFactory<String, byte[]> consumerFactory;
    private final @NonNull String topic;
    private final @NonNull String groupId;
    private final int concurrency;
    private final @NonNull Map<String, Object> defaults;
    private ConcurrentMessageListenerContainer<String, byte[]> container;

    @Override
    public synchronized void start(Consumer<Message> dispatcher) {
        ContainerProperties cp = new ContainerProperties(topic);
        cp.setGroupId(groupId);
        cp.setAckMode(ContainerProperties.AckMode.RECORD);
        cp.setMessageListener((MessageListener<String, byte[]>) rec -> dispatcher.accept(toMessage(rec)));
        container = new ConcurrentMessageListenerContainer<>(consumerFactory, cp);
        container.setConcurrency(concurrency);
        container.setBeanName("kafka-" + name);
        container.start();
        log.info("Inbound channel '{}' listening on topic {} (group {}, concurrency {})",
            name, topic, groupId, concurrency);
    }

    private Message toMessage(ConsumerRecord<String, byte[]> rec) {
        Map<String, Object> h = new HashMap<>(defaults);
        for (Header header : rec.headers()) {
            if (header.key().startsWith("__")) continue; // Spring internal type headers
            h.put(header.key(), header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8));
        }
        FormatSniffer.apply(rec.value(), h, rec.headers().lastHeader(Headers.INPUT_FORMAT) != null);
        h.put(Headers.SOURCE_CHANNEL, name);
        h.put("kafka.topic", rec.topic());
        h.put("kafka.partition", rec.partition());
        h.put("kafka.offset", rec.offset());
        if (rec.key() != null) h.put("kafka.key", rec.key());
        return new Message(rec.value() == null ? new byte[0] : rec.value(), h);
    }

    @Override
    public synchronized void stop() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }
}
