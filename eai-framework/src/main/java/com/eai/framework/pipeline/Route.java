package com.eai.framework.pipeline;

import java.util.List;
import java.util.Objects;

import com.eai.framework.spi.Enricher;
import com.eai.framework.spi.ErrorHandler;
import com.eai.framework.spi.Processor;
import com.eai.framework.spi.Validator;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;

/**
 * Route definition: ordered steps applied to every message, whatever its channel/format.
 * Default output format/channel/destination are used when the input element does not request them.
 */
@Getter
@Builder
public final class Route {

    private final @NonNull String name;
    @Singular("validate") private final List<Validator> validators;
    @Singular("process") private final List<Processor> processors;
    @Singular("enrich") private final List<Enricher> enrichers;
    @Builder.Default private final @NonNull ErrorHandler errorHandler = ex -> { };
    private final String defaultOutputFormat;
    private final String defaultOutputChannel;
    private final String defaultOutputDestination;

    public static RouteBuilder builder(String name) { return new RouteBuilder().name(name); }

    public static class RouteBuilder {
        public RouteBuilder onError(ErrorHandler h) { return errorHandler(Objects.requireNonNull(h)); }

        public RouteBuilder defaultOutput(String format, String channel, String destination) {
            return defaultOutputFormat(format).defaultOutputChannel(channel).defaultOutputDestination(destination);
        }
    }
}
