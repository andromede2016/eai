package com.eai.person.error;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import org.springframework.jms.core.JmsTemplate;

import com.eai.framework.core.Exchange;
import com.eai.framework.core.Message;
import com.eai.framework.spi.ErrorHandler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Sends the original payload of rejected / failed messages to a dead-letter queue,
 * with the reason in JMS properties (eai_status, eai_errors, eai_failure).
 */
@Slf4j
@RequiredArgsConstructor
public class DeadLetterErrorHandler implements ErrorHandler {

    private final JmsTemplate jmsTemplate;
    private final String deadLetterQueue;

    @Override
    public void handle(Exchange ex) {
        Message m = ex.getMessage();
        String errors = ex.getValidationErrors().stream()
            .map(e -> e.field() + ": " + e.message()).collect(Collectors.joining("; "));
        String failure = ex.getFailure() == null ? "" : ex.getFailure().toString();
        log.warn("Message {} {} {} {}", m.getId(), ex.getStatus(), errors, failure);

        if (deadLetterQueue == null || deadLetterQueue.isBlank()) return;
        byte[] original = m.getOriginalPayload() == null ? new byte[0] : m.getOriginalPayload();
        jmsTemplate.send(deadLetterQueue, session -> {
            jakarta.jms.Message dlq = session.createTextMessage(new String(original, StandardCharsets.UTF_8));
            dlq.setStringProperty("eai_status", ex.getStatus().name());
            dlq.setStringProperty("eai_errors", errors);
            dlq.setStringProperty("eai_failure", failure);
            dlq.setStringProperty("eai_messageId", m.getId());
            Object jmsId = m.getHeader("jms.messageId");
            if (jmsId != null) dlq.setStringProperty("eai_originalJmsId", jmsId.toString());
            return dlq;
        });
    }
}
