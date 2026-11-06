package com.eai.framework.engine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import com.eai.framework.core.EaiException;
import com.eai.framework.core.Exchange;
import com.eai.framework.core.ExchangeStatus;
import com.eai.framework.core.Headers;
import com.eai.framework.core.Message;
import com.eai.framework.pipeline.Route;
import com.eai.framework.registry.Registry;
import com.eai.framework.spi.Enricher;
import com.eai.framework.spi.FormatCodec;
import com.eai.framework.spi.InboundChannel;
import com.eai.framework.spi.OutboundChannel;
import com.eai.framework.spi.Processor;
import com.eai.framework.spi.SendTransaction;
import com.eai.framework.spi.Validator;

import lombok.extern.java.Log;

/**
 * Kernel: receive -> decode -> validate -> process -> enrich -> encode -> send.
 */
@Log
public class IntegrationEngine implements AutoCloseable {


    private final Registry<FormatCodec> codecs = new Registry<>("codec", FormatCodec::format);
    private final Registry<OutboundChannel> outbounds = new Registry<>("outbound channel", OutboundChannel::type);
    private final Map<String, Route> routes = new ConcurrentHashMap<>();
    private final List<InboundChannel> inbounds = new CopyOnWriteArrayList<>();
    private volatile String defaultRoute;
    private volatile SendTransaction sendTransaction = SendTransaction.NONE;

    public IntegrationEngine codec(FormatCodec c) { codecs.register(c); return this; }
    public IntegrationEngine outbound(OutboundChannel c) { outbounds.register(c); return this; }
    public IntegrationEngine inbound(InboundChannel c) { inbounds.add(c); return this; }
    /** Runs the encode+send step(s) of each message inside this transaction (default: none). */
    public IntegrationEngine sendTransaction(SendTransaction tx) { this.sendTransaction = tx; return this; }
    public IntegrationEngine route(Route r) {
        routes.put(r.getName(), r);
        if (defaultRoute == null) defaultRoute = r.getName();
        return this;
    }

    public void start() { inbounds.forEach(in -> in.start(this::dispatch)); }

    @Override
    public void close() { inbounds.forEach(InboundChannel::stop); }

    /** Entry point for any inbound channel. */
    public Exchange dispatch(Message message) {
        String routeName = message.getHeaderAsString(Headers.ROUTE);
        String effectiveRoute = routeName != null ? routeName : defaultRoute;
        Route route = routes.get(effectiveRoute);
        Exchange ex = new Exchange(message);

        trace(ex, "RECEIVED from=%s headers=%s payloadSize=%d",
            message.getHeaderAsString(Headers.SOURCE_CHANNEL), message.getHeaders(), size(message.getRawPayload()));
        traceFine(ex, "RECEIVED payload=%s", preview(message.getOriginalPayload()));

        if (route == null) {
            ex.setFailure(new EaiException("No route found: " + effectiveRoute));
            trace(ex, "FAILED no route '%s' (known: %s)", effectiveRoute, routes.keySet());
            return ex;
        }
        trace(ex, "ROUTE %s%s", route.getName(), routeName == null ? " (default)" : "");
        try {
            decode(ex);
            if (!validate(ex, route)) {
                trace(ex, "REJECTED errors=%s -> errorHandler=%s",
                    ex.getValidationErrors(), name(route.getErrorHandler()));
                route.getErrorHandler().handle(ex);
                done(ex);
                return ex;
            }
            for (Processor p : route.getProcessors()) {
                p.process(ex);
                trace(ex, "PROCESSED by %s -> body=%s", name(p), type(message.getBody()));
            }
            ex.setStatus(ExchangeStatus.PROCESSED);
            for (Enricher e : route.getEnrichers()) {
                e.enrich(ex);
                trace(ex, "ENRICHED by %s", name(e));
            }
            ex.setStatus(ExchangeStatus.ENRICHED);
            traceFine(ex, "BODY %s", message.getBody());
            List<String> channels = outputChannels(ex.getMessage(), route);
            if (channels.size() == 1) {
                sendTransaction.execute(() -> { encode(ex, route); send(ex, route); });
            } else {
                sendTransaction.execute(() -> sendToAll(ex, route, channels));
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "[" + message.getId() + "] FAILED at step after " + ex.getStatus()
                + ": " + e + " -> errorHandler=" + name(route.getErrorHandler()), e);
            ex.setFailure(e);
            route.getErrorHandler().handle(ex);
        }
        done(ex);
        return ex;
    }

    private void decode(Exchange ex) throws Exception {
        Message m = ex.getMessage();
        String fmt = m.getHeaderAsString(Headers.INPUT_FORMAT);
        if (fmt != null && m.getRawPayload() != null) {
            FormatCodec codec = codecs.get(fmt);
            m.setBody(codec.decode(m.getRawPayload(), m));
            trace(ex, "DECODED format=%s codec=%s -> body=%s", fmt, name(codec), type(m.getBody()));
        } else {
            trace(ex, "DECODED skipped (no input format header)");
        }
        ex.setStatus(ExchangeStatus.DECODED);
    }

    private boolean validate(Exchange ex, Route route) {
        for (Validator v : route.getValidators()) {
            var errors = v.validate(ex);
            ex.getValidationErrors().addAll(errors);
            trace(ex, "VALIDATED by %s -> %s", name(v), errors.isEmpty() ? "OK" : errors.size() + " error(s) " + errors);
        }
        boolean ok = ex.getValidationErrors().isEmpty();
        ex.setStatus(ok ? ExchangeStatus.VALIDATED : ExchangeStatus.REJECTED);
        return ok;
    }

    private void encode(Exchange ex, Route route) throws Exception {
        Message m = ex.getMessage();
        String requested = m.getHeaderAsString(Headers.OUTPUT_FORMAT);
        String fmt = requested != null ? requested : route.getDefaultOutputFormat();
        if (fmt == null) throw new EaiException("No output format requested");
        m.setHeader(Headers.OUTPUT_FORMAT, fmt);
        FormatCodec codec = codecs.get(fmt);
        m.setRawPayload(codec.encode(m.getBody(), m));
        trace(ex, "ENCODED format=%s%s codec=%s size=%d", fmt, requested == null ? " (route default)" : " (requested)",
            name(codec), size(m.getRawPayload()));
        traceFine(ex, "ENCODED payload=%s", preview(m.getRawPayload()));
        ex.setStatus(ExchangeStatus.ENCODED);
    }

    private void send(Exchange ex, Route route) throws Exception {
        Message m = ex.getMessage();
        String channel = headerOr(m, Headers.OUTPUT_CHANNEL, route.getDefaultOutputChannel());
        if (channel == null) throw new EaiException("No output channel requested");
        // The route's default destination belongs to the route's default channel (e.g. table PERSON for "db").
        // If another channel is requested without a destination, pass null: the channel uses its own default.
        boolean defaultChannel = channel.equals(route.getDefaultOutputChannel());
        String dest = headerOr(m, Headers.OUTPUT_DESTINATION, defaultChannel ? route.getDefaultOutputDestination() : null);
        OutboundChannel out = outbounds.get(channel);
        trace(ex, "SENDING channel=%s (%s) destination=%s", channel, name(out), dest);
        out.send(ex, dest);
        ex.setStatus(ExchangeStatus.SENT);
        trace(ex, "SENT channel=%s destination=%s", channel, dest);
    }

    /** Header eai.output.channel may hold a comma-separated list, e.g. "db,kafka". */
    private static List<String> outputChannels(Message m, Route route) throws EaiException {
        String v = headerOr(m, Headers.OUTPUT_CHANNEL, route.getDefaultOutputChannel());
        if (v == null) throw new EaiException("No output channel requested");
        List<String> list = java.util.Arrays.stream(v.split(","))
            .map(String::trim).filter(s -> !s.isEmpty()).distinct().toList();
        if (list.isEmpty()) throw new EaiException("No output channel requested");
        return list;
    }

    /**
     * Fan-out: same body sent to several channels, in the requested order.
     * Each channel uses its own default format and destination (eai.output.format and
     * eai.output.destination are ignored, they cannot fit several channels).
     * Any failure aborts the remaining channels; the SendTransaction rolls back what it can.
     */
    private void sendToAll(Exchange ex, Route route, List<String> channels) throws Exception {
        Message m = ex.getMessage();
        trace(ex, "FAN-OUT to %s", channels);
        for (String channel : channels) {
            OutboundChannel out = outbounds.get(channel);
            String fmt = out.defaultFormat() != null ? out.defaultFormat() : route.getDefaultOutputFormat();
            if (fmt == null) throw new EaiException("No output format for channel " + channel);
            m.setHeader(Headers.OUTPUT_FORMAT, fmt);
            FormatCodec codec = codecs.get(fmt);
            m.setRawPayload(codec.encode(m.getBody(), m));
            trace(ex, "ENCODED format=%s (channel default) codec=%s size=%d", fmt, name(codec), size(m.getRawPayload()));
            ex.setStatus(ExchangeStatus.ENCODED);
            String dest = channel.equals(route.getDefaultOutputChannel()) ? route.getDefaultOutputDestination() : null;
            trace(ex, "SENDING channel=%s (%s) destination=%s", channel, name(out), dest);
            out.send(ex, dest);
            trace(ex, "SENT channel=%s destination=%s", channel, dest);
        }
        ex.setStatus(ExchangeStatus.SENT);
    }

    // ------------------------------------------------------------------ tracing

    /** Max payload chars printed at FINE (DEBUG) level. Override with -Deai.trace.payload.max=... */
    private static final int PREVIEW_MAX = Integer.getInteger("eai.trace.payload.max", 2000);

    private void done(Exchange ex) {
        long ms = java.time.Duration.between(ex.getStartedAt(), java.time.Instant.now()).toMillis();
        trace(ex, "DONE status=%s in %d ms", ex.getStatus(), ms);
    }

    private static void trace(Exchange ex, String fmt, Object... args) {
        if (log.isLoggable(Level.INFO)) log.info("[" + ex.getMessage().getId() + "] " + String.format(fmt, args));
    }

    private static void traceFine(Exchange ex, String fmt, Object... args) {
        if (log.isLoggable(Level.FINE)) log.fine("[" + ex.getMessage().getId() + "] " + String.format(fmt, args));
    }

    private static String name(Object o) {
        if (o == null) return "none";
        String n = o.getClass().getSimpleName();
        return n.contains("$$Lambda") || n.contains("$Lambda") ? "lambda@" + o.getClass().getEnclosingClass() : n;
    }

    private static String type(Object o) { return o == null ? "null" : o.getClass().getSimpleName(); }

    private static int size(byte[] b) { return b == null ? 0 : b.length; }

    private static String preview(byte[] b) {
        if (b == null) return "null";
        String s = new String(b, java.nio.charset.StandardCharsets.UTF_8).replaceAll("\\s*\\R\\s*", " ");
        return s.length() > PREVIEW_MAX ? s.substring(0, PREVIEW_MAX) + "...(" + s.length() + " chars)" : s;
    }

    private static String headerOr(Message m, String h, String def) {
        String v = m.getHeaderAsString(h);
        return v != null ? v : def;
    }
}
