package com.eai.framework.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/** Execution context of one message through a route. */
@Getter
@RequiredArgsConstructor
public final class Exchange {

    private final Message message;
    private final Instant startedAt = Instant.now();
    private final Map<String, Object> properties = new HashMap<>();
    private final List<ValidationError> validationErrors = new ArrayList<>();
    @Setter
    private ExchangeStatus status = ExchangeStatus.RECEIVED;
    private Throwable failure;

    public void setFailure(Throwable failure) { this.failure = failure; this.status = ExchangeStatus.FAILED; }
}
