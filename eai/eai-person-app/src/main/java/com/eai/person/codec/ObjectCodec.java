package com.eai.person.codec;

import com.eai.framework.core.Message;
import com.eai.framework.spi.FormatCodec;

/** Pass-through "format" when the target consumes the Java object directly (e.g. DB insert). */
public class ObjectCodec implements FormatCodec {
    @Override public String format() { return "object"; }
    @Override public Object decode(byte[] raw, Message ctx) { return raw; }
    @Override public byte[] encode(Object body, Message ctx) { return new byte[0]; }
}
