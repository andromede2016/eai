package com.eai.framework.support;

import java.nio.file.Files;
import java.nio.file.Path;

import com.eai.framework.core.Exchange;
import com.eai.framework.core.Headers;
import com.eai.framework.core.Message;
import com.eai.framework.spi.OutboundChannel;

/** Reference implementation: writes the payload as a file into the destination folder. */
public class FolderOutboundChannel implements OutboundChannel {

    @Override public String type() { return "file"; }

    @Override
    public void send(Exchange exchange, String destination) throws Exception {
        Message m = exchange.getMessage();
        Path dir = Files.createDirectories(Path.of(destination));
        Files.write(dir.resolve(m.getId() + "." + m.getHeaderAsString(Headers.OUTPUT_FORMAT)), m.getRawPayload());
    }
}
