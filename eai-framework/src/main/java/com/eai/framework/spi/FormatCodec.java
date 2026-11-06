package com.eai.framework.spi;

import com.eai.framework.core.Message;

/** Converts raw bytes of a format (csv, xml, json...) to/from a domain object. */
public interface FormatCodec {
    String format();
    Object decode(byte[] raw, Message context) throws Exception;
    byte[] encode(Object body, Message context) throws Exception;
}
