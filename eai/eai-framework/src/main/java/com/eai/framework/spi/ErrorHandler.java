package com.eai.framework.spi;

import com.eai.framework.core.Exchange;

/** Called on rejection (validation) or failure: dead letter, error reply, logging... */
@FunctionalInterface
public interface ErrorHandler {
    void handle(Exchange exchange);
}
