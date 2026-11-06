package com.eai.person;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/** Kafka as source (person.in -> DB), as target (JMS -> person.out) and DLT (person.dlt). */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.artemis.mode=embedded",
        "spring.artemis.embedded.queues=PERSON.IN,PERSON.DLQ,PERSON.OUT",
        "spring.datasource.url=jdbc:h2:mem:personkafka;DB_CLOSE_DELAY=-1",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "eai.kafka.concurrency=1"
    })
@EmbeddedKafka(partitions = 1, topics = { "person.in", "person.out", "person.dlt" })
class KafkaFlowTest {

    @Autowired KafkaTemplate<String, byte[]> kafka;
    @Autowired JmsTemplate jms;
    @Autowired JdbcTemplate jdbc;
    @Autowired EmbeddedKafkaBroker broker;

    @BeforeEach
    void clean() { jdbc.update("DELETE FROM PERSON"); }

    @Test
    void jsonFromKafkaIsInsertedInDb() {
        send("person.in", "100", """
            {"id":100,"firstName":"Ada","lastName":"Lovelace","email":"Ada@Example.com","country":"GB"}""");

        Awaitility.await().atMost(Duration.ofSeconds(10))
            .until(() -> jdbc.queryForObject("SELECT COUNT(*) FROM PERSON WHERE ID=100", Integer.class) == 1);
        Map<String, Object> row = jdbc.queryForMap("SELECT * FROM PERSON WHERE ID=100");
        assertThat(row.get("FULL_NAME")).isEqualTo("Ada LOVELACE");
        assertThat(row.get("SOURCE")).isEqualTo("kafka-person");
    }

    @Test
    void xmlFromJmsCanTargetKafkaAsJson() {
        jms.convertAndSend("PERSON.IN",
            "<person><id>200</id><firstName>Alan</firstName><lastName>Turing</lastName></person>", msg -> {
                msg.setStringProperty("eai_output_channel", "kafka");
                msg.setStringProperty("eai_output_format", "json");
                msg.setStringProperty("eai_output_destination", "person.out");
                return msg;
            });

        try (Consumer<String, byte[]> c = consumer("person.out")) {
            ConsumerRecord<String, byte[]> rec = KafkaTestUtils.getSingleRecord(c, "person.out", Duration.ofSeconds(10));
            assertThat(rec.key()).isEqualTo("200");
            assertThat(new String(rec.value(), StandardCharsets.UTF_8))
                .contains("\"fullName\":\"Alan TURING\"", "\"source\":\"jms-person\"");
        }
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM PERSON", Integer.class)).isZero();
    }

    @Test
    void jmsToKafkaWithoutDestinationUsesDefaultTopic() {
        jms.convertAndSend("PERSON.IN",
            "<person><id>300</id><firstName>Grace</firstName><lastName>Hopper</lastName></person>", msg -> {
                msg.setStringProperty("eai_output_channel", "kafka");
                return msg;
            });
        try (Consumer<String, byte[]> c = consumer("person.out")) {
            var recs = KafkaTestUtils.getRecords(c, Duration.ofSeconds(10));
            assertThat(recs.records("person.out")).anyMatch(r -> "300".equals(r.key())
                && new String(r.value(), StandardCharsets.UTF_8).contains("\"fullName\":\"Grace HOPPER\""));
        }
    }

    @Test
    void jmsToDbAndKafka() {
        jms.convertAndSend("PERSON.IN",
            "<person><id>400</id><firstName>Linus</firstName><lastName>Torvalds</lastName></person>", msg -> {
                msg.setStringProperty("eai_output_channel", "db,kafka");
                return msg;
            });
        Awaitility.await().atMost(Duration.ofSeconds(10))
            .until(() -> jdbc.queryForObject("SELECT COUNT(*) FROM PERSON WHERE ID=400", Integer.class) == 1);
        try (Consumer<String, byte[]> c = consumer("person.out")) {
            var recs = KafkaTestUtils.getRecords(c, Duration.ofSeconds(10));
            assertThat(recs.records("person.out")).anyMatch(r -> "400".equals(r.key())
                && new String(r.value(), StandardCharsets.UTF_8).contains("\"fullName\":\"Linus TORVALDS\""));
        }
    }

    @Test
    void kafkaFailureRollsBackDbInsert() throws Exception {
        // 'nope' channel doesn't exist -> fails after the DB insert -> insert must be rolled back
        jms.convertAndSend("PERSON.IN",
            "<person><id>500</id><firstName>X</firstName><lastName>Y</lastName></person>", msg -> {
                msg.setStringProperty("eai_output_channel", "db,nope");
                return msg;
            });
        jms.setReceiveTimeout(10000);
        assertThat(jms.receive("PERSON.DLQ")).isNotNull();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM PERSON WHERE ID=500", Integer.class)).isZero();
    }

    @Test
    void invalidKafkaRecordGoesToDeadLetterTopic() {
        String bad = "{\"id\":\"abc\",\"lastName\":\"X\"}";
        send("person.in", "bad", bad);

        try (Consumer<String, byte[]> c = consumer("person.dlt")) {
            ConsumerRecord<String, byte[]> rec = KafkaTestUtils.getSingleRecord(c, "person.dlt", Duration.ofSeconds(10));
            assertThat(new String(rec.value(), StandardCharsets.UTF_8)).isEqualTo(bad);
            assertThat(header(rec, "eai.status")).isEqualTo("REJECTED");
            assertThat(header(rec, "eai.errors")).contains("id: must be numeric", "firstName: required");
            assertThat(header(rec, "eai.original.topic")).isEqualTo("person.in");
        }
    }

    private void send(String topic, String key, String json) {
        kafka.send(new ProducerRecord<>(topic, key, json.getBytes(StandardCharsets.UTF_8))).join();
    }

    private Consumer<String, byte[]> consumer(String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps("test-" + topic, "false", broker);
        props.put("auto.offset.reset", "earliest");
        Consumer<String, byte[]> c = new DefaultKafkaConsumerFactory<>(props,
            new StringDeserializer(), new ByteArrayDeserializer()).createConsumer();
        broker.consumeFromAnEmbeddedTopic(c, topic);
        return c;
    }

    private static String header(ConsumerRecord<?, ?> rec, String name) {
        var h = rec.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
