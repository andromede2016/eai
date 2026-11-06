package com.eai.framework.support;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.eai.framework.core.Headers;
import com.eai.framework.core.Message;
import com.eai.framework.spi.InboundChannel;

/** Reference implementation: polls a folder; input format = file extension; file deleted after read. */
public class FolderInboundChannel implements InboundChannel {

    private final String name;
    private final Path folder;
    private final long pollMillis;
    private final Map<String, Object> staticHeaders;
    private ScheduledExecutorService scheduler;

    public FolderInboundChannel(String name, Path folder, long pollMillis, Map<String, Object> staticHeaders) {
        this.name = name; this.folder = folder; this.pollMillis = pollMillis;
        this.staticHeaders = staticHeaders == null ? Map.of() : staticHeaders;
    }

    @Override public String name() { return name; }

    @Override
    public void start(Consumer<Message> dispatcher) {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> poll(dispatcher), 0, pollMillis, TimeUnit.MILLISECONDS);
    }

    private void poll(Consumer<Message> dispatcher) {
        try (Stream<Path> files = Files.list(folder)) {
            files.filter(Files::isRegularFile).forEach(f -> {
                try {
                    String fn = f.getFileName().toString();
                    Map<String, Object> h = new HashMap<>(staticHeaders);
                    h.put(Headers.SOURCE_CHANNEL, name);
                    h.put("file.name", fn);
                    int dot = fn.lastIndexOf('.');
                    if (dot > 0) h.putIfAbsent(Headers.INPUT_FORMAT, fn.substring(dot + 1));
                    byte[] data = Files.readAllBytes(f);
                    Files.delete(f);
                    dispatcher.accept(new Message(data, h));
                } catch (Exception ignored) { /* skip file */ }
            });
        } catch (Exception ignored) { /* folder unavailable */ }
    }

    @Override public void stop() { if (scheduler != null) scheduler.shutdownNow(); }
}
