package com.eai.framework.spi;

/**
 * Wraps the "send" step so that all output channels of one message run in one transaction.
 * If any channel throws, the work must be rolled back (e.g. the DB insert) and the exception rethrown.
 * The default implementation runs the work without any transaction.
 */
@FunctionalInterface
public interface SendTransaction {

    @FunctionalInterface
    interface Work { void run() throws Exception; }

    void execute(Work work) throws Exception;

    SendTransaction NONE = Work::run;
}
