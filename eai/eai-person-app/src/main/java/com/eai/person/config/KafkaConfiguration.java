package com.eai.person.config;

import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import com.eai.framework.core.Headers;
import com.eai.person.channel.KafkaInboundChannel;
import com.eai.person.channel.KafkaOutboundChannel;
import com.eai.person.error.KafkaDeadLetterErrorHandler;
import com.eai.person.model.Person;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Kafka wiring (source + target + DLT). Disabled with {@code eai.kafka.enabled=false}.
 * ConsumerFactory / KafkaTemplate come from Spring Boot auto-configuration ({@code spring.kafka.*}).
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "eai.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConfiguration {

    /** Source: topic person.in -> same route as JMS. */
    @Bean
    KafkaInboundChannel kafkaPersonInbound(ConsumerFactory<String, byte[]> cf, EaiProperties props) {
        EaiProperties.Kafka k = props.getKafka();
        return new KafkaInboundChannel(k.getName(), cf, k.getInputTopic(), k.getGroupId(), k.getConcurrency(),
            Map.of(Headers.INPUT_FORMAT, k.getInputFormat()));
    }

    /** Target "kafka": key = person id (keeps all events of one person ordered in one partition). */
    @Bean
    KafkaOutboundChannel kafkaOutboundChannel(KafkaTemplate<String, byte[]> kafka, EaiProperties props,
                                              ObjectMapper mapper) {
        return new KafkaOutboundChannel(kafka, props.getKafka().getOutputTopic(), ex -> {
            Object body = ex.getMessage().getBody();
            return body instanceof Person p && p.getId() != null ? p.getId().toString() : ex.getMessage().getId();
        }, mapper);
    }

    @Bean
    KafkaDeadLetterErrorHandler kafkaDeadLetterErrorHandler(KafkaTemplate<String, byte[]> kafka, EaiProperties props) {
        return new KafkaDeadLetterErrorHandler(kafka, props.getKafka().getDeadLetterTopic());
    }
}
