package com.eai.person.config;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import com.eai.framework.engine.IntegrationEngine;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Starts the inbound channels once the context is fully ready, and stops them first on shutdown
 * (graceful: in-flight messages finish before the connection factory / datasource are closed).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EngineLifecycle implements SmartLifecycle {

    private final IntegrationEngine engine;
    private volatile boolean running;

    @Override
    public void start() {
        engine.start();
        running = true;
        log.info("EAI engine started");
    }

    @Override
    public void stop() {
        engine.close();
        running = false;
        log.info("EAI engine stopped");
    }

    @Override public boolean isRunning() { return running; }

    /** Start last, stop first. */
    @Override public int getPhase() { return Integer.MAX_VALUE - 100; }
}
