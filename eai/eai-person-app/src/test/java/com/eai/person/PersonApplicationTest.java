package com.eai.person;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jms.core.JmsTemplate;

import jakarta.jms.TextMessage;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.artemis.mode=embedded",
        "spring.artemis.embedded.queues=PERSON.IN,PERSON.DLQ,PERSON.OUT",
        "spring.datasource.url=jdbc:h2:mem:person;DB_CLOSE_DELAY=-1",
        "eai.kafka.enabled=false"
    })
class PersonApplicationTest {

    @Autowired JmsTemplate jms;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM PERSON");
        jms.setReceiveTimeout(100);
        while (jms.receive("PERSON.DLQ") != null) { /* drain */ }
    }

    @Test
    void validXmlIsInserted() {
        jms.convertAndSend("PERSON.IN", """
            <person>
              <id>42</id>
              <firstName>John</firstName>
              <lastName>Doe</lastName>
              <email>John.Doe@Example.com</email>
              <birthDate>1990-05-17</birthDate>
              <country>FR</country>
            </person>""");

        Awaitility.await().atMost(Duration.ofSeconds(5))
            .until(() -> jdbc.queryForObject("SELECT COUNT(*) FROM PERSON", Integer.class) == 1);

        Map<String, Object> row = jdbc.queryForMap("SELECT * FROM PERSON");
        assertThat(row.get("ID")).isEqualTo(42L);
        assertThat(row.get("FULL_NAME")).isEqualTo("John DOE");
        assertThat(row.get("EMAIL")).isEqualTo("john.doe@example.com");
        assertThat(row.get("COUNTRY")).isEqualTo("FR");
        assertThat(row.get("SOURCE")).isEqualTo("jms-person");
    }

    @Test
    void jsonOnJmsQueueIsDetectedAndInserted() {
        jms.convertAndSend("PERSON.IN",
            "{\"id\":4,\"firstName\":\"Ada\",\"lastName\":\"Lovelace\",\"email\":\"ada@example.com\",\"country\":\"GB\"}");
        Awaitility.await().atMost(Duration.ofSeconds(5))
            .until(() -> jdbc.queryForObject("SELECT COUNT(*) FROM PERSON WHERE ID=4", Integer.class) == 1);
    }

    @Test
    void invalidXmlGoesToDeadLetterQueue() throws Exception {
        String bad = "<person><id>abc</id><lastName>X</lastName></person>";
        jms.convertAndSend("PERSON.IN", bad);

        jms.setReceiveTimeout(5000);
        TextMessage dlq = (TextMessage) jms.receive("PERSON.DLQ");

        assertThat(dlq).isNotNull();
        assertThat(dlq.getText()).isEqualTo(bad);
        assertThat(dlq.getStringProperty("eai_status")).isEqualTo("REJECTED");
        assertThat(dlq.getStringProperty("eai_errors")).contains("id: must be numeric", "firstName: required");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM PERSON", Integer.class)).isZero();
    }

    @Test
    void jmsToJmsAsXml() throws Exception {
        jms.convertAndSend("PERSON.IN",
            "<person><id>600</id><firstName>Ada</firstName><lastName>Lovelace</lastName></person>", msg -> {
                msg.setStringProperty("eai_output_channel", "jms");
                msg.setStringProperty("eai_output_format", "xml");
                return msg;
            });
        jms.setReceiveTimeout(5000);
        TextMessage out = (TextMessage) jms.receive("PERSON.OUT");
        assertThat(out).isNotNull();
        assertThat(out.getText()).contains("<person>", "<id>600</id>", "<fullName>Ada LOVELACE</fullName>");
        assertThat(out.getStringProperty("content_format")).isEqualTo("xml");
        assertThat(out.getJMSCorrelationID()).isNotBlank();
    }

    @Test
    void jmsToDbAndJms() throws Exception {
        jms.convertAndSend("PERSON.IN",
            "<person><id>601</id><firstName>Alan</firstName><lastName>Turing</lastName></person>", msg -> {
                msg.setStringProperty("eai_output_channel", "db,jms");
                return msg;
            });
        jms.setReceiveTimeout(5000);
        TextMessage out = (TextMessage) jms.receive("PERSON.OUT");
        assertThat(out).isNotNull();
        assertThat(out.getText()).contains("<fullName>Alan TURING</fullName>");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM PERSON WHERE ID=601", Integer.class)).isEqualTo(1);
    }

    @Test
    void sendingToInboundQueueIsRefused() throws Exception {
        jms.convertAndSend("PERSON.IN",
            "<person><id>602</id><firstName>A</firstName><lastName>B</lastName></person>", msg -> {
                msg.setStringProperty("eai_output_channel", "jms");
                msg.setStringProperty("eai_output_destination", "PERSON.IN");
                return msg;
            });
        jms.setReceiveTimeout(5000);
        jakarta.jms.Message dlq = jms.receive("PERSON.DLQ");
        assertThat(dlq).isNotNull();
        assertThat(dlq.getStringProperty("eai_failure")).contains("message loop");
    }

    @Test
    void sameIdTwiceCreatesTwoRowsWithDifferentDbId() throws Exception {
        String xml = "<person><id>7</id><firstName>A</firstName><lastName>B</lastName></person>";
        jms.convertAndSend("PERSON.IN", xml);
        Awaitility.await().atMost(Duration.ofSeconds(5))
            .until(() -> jdbc.queryForObject("SELECT COUNT(*) FROM PERSON", Integer.class) == 1);

        jms.convertAndSend("PERSON.IN", xml);
        Awaitility.await().atMost(Duration.ofSeconds(5))
            .until(() -> jdbc.queryForObject("SELECT COUNT(*) FROM PERSON WHERE ID=7", Integer.class) == 2);

        assertThat(jdbc.queryForObject("SELECT COUNT(DISTINCT DB_ID) FROM PERSON WHERE ID=7", Integer.class))
            .isEqualTo(2);
    }
}
