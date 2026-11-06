package com.eai.framework.spi;

import com.eai.framework.core.Exchange;

/** Output channel (JMS producer, file writer, DB writer, HTTP client...). */
public interface OutboundChannel {
    /** Channel type, matched against header {@code eai.output.channel}. */
    String type();
    /** Sends the encoded raw payload to the given destination. */
    void send(Exchange exchange, String destination) throws Exception;
    /**
     * Format this channel expects when several channels are requested at once
     * (e.g. "object" for a DB writer, "json" for Kafka). null = use the route default.
     */
    default String defaultFormat() { return null; }
}
