package com.eai.person.channel;

import java.nio.charset.StandardCharsets;

import org.springframework.jms.core.JmsTemplate;

import com.eai.framework.core.EaiException;
import com.eai.framework.core.Exchange;
import com.eai.framework.core.Headers;
import com.eai.framework.core.Message;
import com.eai.framework.spi.OutboundChannel;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Target "jms": sends the encoded payload as a JMS TextMessage to the queue given as destination
 * (default PERSON.OUT).
 *
 * <p>JMS property names cannot contain '.', so EAI headers are written with '_'
 * (eai.messageId -> eai_messageId), the same convention {@link JmsInboundChannel} reads.
 *
 * <p>Sending to the inbound queue is refused: the app would consume its own output forever (loop).
 */
@Slf4j
@RequiredArgsConstructor
public class JmsOutboundChannel implements OutboundChannel {

	private final @NonNull JmsTemplate jms;
	private final @NonNull String defaultQueue;
	private final @NonNull String inboundQueue;
	private final @NonNull ObjectMapper mapper;

	@Override
	public String type() {
		return "jms";
	}

	/** Used when several channels are requested (e.g. eai_output_channel=db,jms). */
	@Override
	public String defaultFormat() {
		return "xml";
	}

	@Override
	public void send(Exchange ex, String destination) throws Exception {
		String queue = destination == null ? defaultQueue : destination;
		if (queue.equalsIgnoreCase(inboundQueue)) {
			throw new EaiException("Refusing to send to the inbound queue " + queue + " (message loop)");
		}
		Message m = ex.getMessage();
		byte[] payload = m.getRawPayload();
		String format = m.getHeaderAsString(Headers.OUTPUT_FORMAT);
		// Format "object" (DB default) produces no bytes: a text message needs serialized content -> JSON.
		if ((payload == null || payload.length == 0) && m.getBody() != null) {
			payload = mapper.writeValueAsBytes(m.getBody());
			format = "json";
		}
		String text = payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
		String contentFormat = format;

		jms.send(queue, session -> {
			jakarta.jms.TextMessage out = session.createTextMessage(text);
			out.setStringProperty("eai_messageId", m.getId());
			setIfNotNull(out, "eai_source_channel", m.getHeaderAsString(Headers.SOURCE_CHANNEL));
			setIfNotNull(out, "content_format", contentFormat);
			// Correlation: lets a consumer link this message to the one we received.
			Object jmsId = m.getHeader("jms.messageId");
			if (jmsId != null) out.setJMSCorrelationID(jmsId.toString());
			return out;
		});
		log.info("[{}] sent to JMS queue {} format={} size={}", m.getId(), queue, contentFormat, text.length());
	}

	private static void setIfNotNull(jakarta.jms.Message msg, String name, String value)
			throws jakarta.jms.JMSException {
		if (value != null) msg.setStringProperty(name, value);
	}
}
