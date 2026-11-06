package com.eai.person.error;

import com.eai.framework.core.Exchange;
import com.eai.framework.spi.ErrorHandler;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Sends failures back to the technology they came from:
 * records received from Kafka (header {@code kafka.topic}) go to the Kafka DLT, everything else to the JMS DLQ.
 */
@RequiredArgsConstructor
public class SourceAwareErrorHandler implements ErrorHandler {

    private final @NonNull ErrorHandler jms;
    /** May be null when Kafka is disabled. */
    private final ErrorHandler kafka;

    @Override
    public void handle(Exchange ex) {
        boolean fromKafka = ex.getMessage().getHeader("kafka.topic") != null;
        (fromKafka && kafka != null ? kafka : jms).handle(ex);
    }
}
