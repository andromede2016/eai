package com.eai.framework.core;

import lombok.experimental.UtilityClass;

/** Well-known header names. The input element carries the requested output format/target. */
@UtilityClass
public class Headers {
    /** Format of the incoming payload (csv, xml, json...). */
    public final String INPUT_FORMAT = "eai.input.format";
    /** Format requested for the output. */
    public final String OUTPUT_FORMAT = "eai.output.format";
    /** Channel type requested for the output (jms, file, db, http...). */
    public final String OUTPUT_CHANNEL = "eai.output.channel";
    /** Destination within the output channel (queue, folder, table, url...). */
    public final String OUTPUT_DESTINATION = "eai.output.destination";
    /** Name of the inbound channel that received the message. */
    public final String SOURCE_CHANNEL = "eai.source.channel";
    /** Route to use (default route if absent). */
    public final String ROUTE = "eai.route";
}
