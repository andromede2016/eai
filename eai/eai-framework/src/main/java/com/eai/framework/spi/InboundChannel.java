package com.eai.framework.spi;

import java.util.function.Consumer;

import com.eai.framework.core.Message;

/**
 * Input channel (JMS listener, folder poller, REST endpoint...).
 * Implementations push received messages to the given dispatcher (the engine).
 */
public interface InboundChannel extends AutoCloseable {
    String name();
    void start(Consumer<Message> dispatcher);
    void stop();
    @Override default void close() { stop(); }
}
