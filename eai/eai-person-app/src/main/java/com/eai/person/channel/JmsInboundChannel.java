package com.eai.person.channel;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.springframework.jms.listener.DefaultMessageListenerContainer;

import com.eai.framework.core.Headers;
import com.eai.framework.core.Message;
import com.eai.framework.spi.InboundChannel;

import jakarta.jms.BytesMessage;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * JMS input channel backed by Spring's {@link DefaultMessageListenerContainer}
 * (concurrent consumers, transacted session, automatic reconnection).
 * JMS properties are copied to headers with '_' replaced by '.' (JMS names can't contain dots),
 * e.g. {@code eai_output_destination} -> {@code eai.output.destination}.
 */
@Slf4j
@RequiredArgsConstructor
public class JmsInboundChannel implements InboundChannel {

    @Getter @Accessors(fluent = true)
    private final @NonNull String name;
    private final @NonNull ConnectionFactory connectionFactory;
    private final @NonNull String queue;
    private final @NonNull String concurrency;
    private final @NonNull Map<String, Object> defaults;
    private DefaultMessageListenerContainer container;

    @Override
    public synchronized void start(Consumer<Message> dispatcher) {
        container = new DefaultMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setDestinationName(queue);
        container.setConcurrency(concurrency);
        container.setSessionTransacted(true);
        container.setMessageListener((MessageListener) jms -> {
            try {
                dispatcher.accept(toMessage(jms));
            } catch (JMSException e) {
                throw new IllegalStateException("Cannot read JMS message", e); // rollback -> redelivery
            }
        });
        container.afterPropertiesSet();
        container.start();
        log.info("Inbound channel '{}' listening on queue {} (concurrency {})", name, queue, concurrency);
    }

    private Message toMessage(jakarta.jms.Message jms) throws JMSException {
        byte[] payload;
        if (jms instanceof TextMessage t) {
            payload = t.getText() == null ? new byte[0] : t.getText().getBytes(StandardCharsets.UTF_8);
        } else if (jms instanceof BytesMessage b) {
            payload = new byte[(int) b.getBodyLength()];
            b.readBytes(payload);
        } else {
            throw new JMSException("Unsupported JMS message type: " + jms.getClass().getName());
        }
        Map<String, Object> h = new HashMap<>(defaults);
        boolean explicitFormat = false;
        for (Object n : Collections.list(jms.getPropertyNames())) {
            String key = n.toString();
            String name = key.replace('_', '.');
            h.put(name, jms.getObjectProperty(key));
            explicitFormat |= Headers.INPUT_FORMAT.equals(name);
        }
        FormatSniffer.apply(payload, h, explicitFormat);
        h.put(Headers.SOURCE_CHANNEL, name);
        h.put("jms.messageId", jms.getJMSMessageID());
        return new Message(payload, h);
    }

    @Override
    public synchronized void stop() {
        if (container != null) {
            container.shutdown();
            container = null;
        }
    }
}
