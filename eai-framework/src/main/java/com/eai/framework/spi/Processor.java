package com.eai.framework.spi;

import com.eai.framework.core.Exchange;

/** Business task: DB insert/update, send mail, call a service... */
@FunctionalInterface
public interface Processor {
    void process(Exchange exchange) throws Exception;
}
