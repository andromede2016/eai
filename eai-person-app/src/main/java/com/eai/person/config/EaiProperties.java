package com.eai.person.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import lombok.Data;

/** Externalised configuration, prefix {@code eai}. */
@Data
@Validated
@ConfigurationProperties(prefix = "eai")
public class EaiProperties {

    private Inbound inbound = new Inbound();
    private Output output = new Output();
    /** Queue receiving rejected / failed messages (empty = log only). */
    private String deadLetterQueue = "PERSON.DLQ";
    /** Default queue for output channel "jms" when no destination is given. */
    private String jmsOutputQueue = "PERSON.OUT";
    private Kafka kafka = new Kafka();

    @Data
    public static class Kafka {
        /** Master switch: Kafka inbound channel, outbound channel "kafka" and DLT. */
        private boolean enabled = true;
        /** Logical channel name, stored in PERSON.SOURCE. */
        private String name = "kafka-person";
        private String inputTopic = "person.in";
        /** Consumer group: instances with the same group share the partitions. */
        private String groupId = "eai-person-app";
        /** Consumer threads (effective max = number of partitions). */
        private int concurrency = 1;
        /** Format of records on the input topic (json or xml). */
        private String inputFormat = "json";
        /** Default topic for channel "kafka" when no destination is given. */
        private String outputTopic = "person.out";
        /** Dead letter topic for rejected / failed Kafka records (empty = log only). */
        private String deadLetterTopic = "person.dlt";
    }

    @Data
    public static class Inbound {
        /** Logical channel name, stored in PERSON.SOURCE. */
        private String name = "jms-person";
        private String queue = "PERSON.IN";
        /** Listener concurrency, e.g. "1-5". */
        private String concurrency = "1-5";
        private String inputFormat = "xml";
    }

    @Data
    public static class Output {
        private String format = "object";
        private String channel = "db";
        private String destination = "PERSON";
    }
}
