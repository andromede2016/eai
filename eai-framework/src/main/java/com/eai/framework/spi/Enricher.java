package com.eai.framework.spi;

import com.eai.framework.core.Exchange;

/** Adds / transforms data on the message body or headers. */
@FunctionalInterface
public interface Enricher {
    void enrich(Exchange exchange) throws Exception;
}
