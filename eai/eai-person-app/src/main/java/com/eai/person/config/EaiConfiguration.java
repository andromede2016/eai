package com.eai.person.config;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import com.eai.person.codec.JsonCodec;
import com.eai.person.error.KafkaDeadLetterErrorHandler;
import com.eai.person.error.SourceAwareErrorHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.eai.framework.spi.SendTransaction;

import com.eai.framework.core.Headers;
import com.eai.framework.engine.IntegrationEngine;
import com.eai.framework.pipeline.Route;
import com.eai.framework.spi.Enricher;
import com.eai.framework.spi.ErrorHandler;
import com.eai.framework.spi.FormatCodec;
import com.eai.framework.spi.InboundChannel;
import com.eai.framework.spi.OutboundChannel;
import com.eai.framework.spi.Processor;
import com.eai.framework.spi.Validator;
import com.eai.person.channel.JdbcPersonOutboundChannel;
import com.eai.person.channel.JmsInboundChannel;
import com.eai.person.channel.JmsOutboundChannel;
import com.eai.person.codec.ObjectCodec;
import com.eai.person.codec.XmlCodec;
import com.eai.person.enrich.PersonEnricher;
import com.eai.person.error.DeadLetterErrorHandler;
import com.eai.person.processor.PersonMappingProcessor;
import com.eai.person.validation.PersonXmlValidator;

import jakarta.jms.ConnectionFactory;

/** Wires the framework components as Spring beans. */
@Configuration(proxyBeanMethods = false)
public class EaiConfiguration {

    // ---- formats
    @Bean XmlCodec xmlCodec(ObjectMapper mapper) { return new XmlCodec(mapper); }
    @Bean ObjectCodec objectCodec() { return new ObjectCodec(); }
    @Bean JsonCodec jsonCodec(ObjectMapper mapper) { return new JsonCodec(mapper); }

    // ---- pipeline steps
    @Bean PersonXmlValidator personXmlValidator() { return new PersonXmlValidator(); }
    @Bean PersonMappingProcessor personMappingProcessor() { return new PersonMappingProcessor(); }
    @Bean PersonEnricher personEnricher() { return new PersonEnricher(); }

    // ---- channels
    @Bean
    JdbcPersonOutboundChannel jdbcPersonOutboundChannel(JdbcTemplate jdbcTemplate) {
        return new JdbcPersonOutboundChannel(jdbcTemplate);
    }

    @Bean
    JmsOutboundChannel jmsOutboundChannel(JmsTemplate jmsTemplate, EaiProperties props, ObjectMapper mapper) {
        return new JmsOutboundChannel(jmsTemplate, props.getJmsOutputQueue(), props.getInbound().getQueue(), mapper);
    }

    @Bean
    JmsInboundChannel jmsPersonInbound(ConnectionFactory cf, EaiProperties props) {
        EaiProperties.Inbound in = props.getInbound();
        return new JmsInboundChannel(in.getName(), cf, in.getQueue(), in.getConcurrency(),
            Map.of(Headers.INPUT_FORMAT, in.getInputFormat()));
    }
    

    @Bean
    DeadLetterErrorHandler deadLetterErrorHandler(JmsTemplate jmsTemplate, EaiProperties props) {
        return new DeadLetterErrorHandler(jmsTemplate, props.getDeadLetterQueue());
    }

    /** JMS messages -> JMS DLQ, Kafka records -> Kafka DLT. */
    @Bean @Primary
    ErrorHandler sourceAwareErrorHandler(DeadLetterErrorHandler jmsDlq,
                                         ObjectProvider<KafkaDeadLetterErrorHandler> kafkaDlt) {
        return new SourceAwareErrorHandler(jmsDlq, kafkaDlt.getIfAvailable());
    }

    // ---- route
    @Bean
    Route personImportRoute(List<Validator> validators, List<Processor> processors,
                            List<Enricher> enrichers, ErrorHandler errorHandler, EaiProperties props) {
        return Route.builder("person-import")
            .validators(validators)
            .processors(processors)
            .enrichers(enrichers)
            .onError(errorHandler)
            .defaultOutput(props.getOutput().getFormat(), props.getOutput().getChannel(),
                props.getOutput().getDestination())
            .build();
    }

    // ---- transaction around the send step(s)
    /**
     * DB insert and Kafka send of one message run in one DB transaction: the Kafka send waits for the
     * broker ack, so if Kafka fails the exception rolls back the insert (no row without its Kafka record).
     */
    @Bean
    SendTransaction sendTransaction(PlatformTransactionManager txManager) {
        TransactionTemplate tx = new TransactionTemplate(txManager);
        return work -> {
            try {
                tx.executeWithoutResult(status -> {
                    try { work.run(); }
                    catch (RuntimeException e) { throw e; }
                    catch (Exception e) { throw new WrappedException(e); }
                });
            } catch (WrappedException e) {
                throw (Exception) e.getCause();
            }
        };
    }

    private static final class WrappedException extends RuntimeException {
        WrappedException(Exception cause) { super(cause); }
    }

    // ---- engine (started/stopped by EngineLifecycle)
    @Bean
    IntegrationEngine integrationEngine(List<FormatCodec> codecs, List<OutboundChannel> outbounds,
                                        List<InboundChannel> inbounds, List<Route> routes,
                                        SendTransaction sendTransaction) {
        IntegrationEngine engine = new IntegrationEngine();
        engine.sendTransaction(sendTransaction);
        codecs.forEach(engine::codec);
        outbounds.forEach(engine::outbound);
        inbounds.forEach(engine::inbound);
        routes.forEach(engine::route);
        return engine;
    }
}
