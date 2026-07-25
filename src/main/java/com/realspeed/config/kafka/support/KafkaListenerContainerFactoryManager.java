package com.realspeed.config.kafka.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.MDC;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchInterceptor;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class KafkaListenerContainerFactoryManager {
    private final KafkaConnectionProperties properties;
    private final Map<String, ConcurrentKafkaListenerContainerFactory<String, String>> stringFactories = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentKafkaListenerContainerFactory<String, Object>> objectFactories = new ConcurrentHashMap<>();

    public KafkaListenerContainerFactoryManager(KafkaConnectionProperties properties) {
        this.properties = properties;
    }

    public ConcurrentKafkaListenerContainerFactory<String, String> getStringFactory(String connectionName) {
        return stringFactories.computeIfAbsent(connectionName, (String name) -> {
            KafkaProperties cfg = properties.getConnections().get(name);
            if (cfg == null) {
                throw new IllegalArgumentException("No Kafka connection configured with name: " + name);
            }

            Map<String, Object> consumerProps = cfg.buildConsumerProperties();
            Object valueDeserializer = consumerProps.get("value.deserializer");

            if (valueDeserializer != null
                    && !valueDeserializer.toString().contains("StringDeserializer")) {
                throw new IllegalStateException(
                        "Connection '" + name + "' is not configured for String deserialization. " +
                                "Current deserializer: " + valueDeserializer);
            }

            // If null, Spring Boot defaults to StringDeserializer, which is acceptable for String factory

            DefaultKafkaConsumerFactory<String, String> consumerFactory =
                    new DefaultKafkaConsumerFactory<>(consumerProps);

            ConcurrentKafkaListenerContainerFactory<String, String> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();

            factory.setConsumerFactory(consumerFactory);
            CommonErrorHandler errorHandler = kafkaCommonErrorHandler();
            factory.setCommonErrorHandler(errorHandler);
            factory.setBatchInterceptor(batchInterceptor());

            // Apply all listener properties from KafkaProperties
            applyListenerProperties(factory, cfg.getListener(), name);

            if(Boolean.TRUE.equals(factory.isBatchListener())) {
                factory.setBatchInterceptor(batchInterceptor());
            }

            log.info("Lazily created String KafkaListenerContainerFactory for connection: {}", name);
            return factory;
        });
    }

    public ConcurrentKafkaListenerContainerFactory<String, Object> getObjectFactory(String connectionName) {
        return objectFactories.computeIfAbsent(connectionName, (String name) -> {
            KafkaProperties cfg = properties.getConnections().get(name);
            if (cfg == null) {
                throw new IllegalArgumentException("No Kafka connection configured with name: " + name);
            }

            Map<String, Object> consumerProps = cfg.buildConsumerProperties();
            Object valueDeserializer = consumerProps.get("value.deserializer");

            // Reject if explicitly set to StringDeserializer, or if null (defaults to StringDeserializer)
            if (valueDeserializer == null
                    || valueDeserializer.toString().contains("StringDeserializer")) {
                throw new IllegalStateException(
                        "Connection '" + name + "' is configured for String, not Object deserialization. " +
                                "Use getStringFactory() instead. Current deserializer: " +
                                (valueDeserializer == null
                                        ? "null (defaults to StringDeserializer)"
                                        : valueDeserializer));
            }

            DefaultKafkaConsumerFactory<String, Object> consumerFactory =
                    new DefaultKafkaConsumerFactory<>(consumerProps);

            ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();

            factory.setConsumerFactory(consumerFactory);

            CommonErrorHandler errorHandler = kafkaCommonErrorHandler();
            factory.setCommonErrorHandler(errorHandler);

            // Apply all listener properties from KafkaProperties
            applyListenerProperties(factory, cfg.getListener(), name);

            if(Boolean.TRUE.equals(factory.isBatchListener())) {
                factory.setBatchInterceptor(batchInterceptor());
            }

            log.info("Lazily created Object KafkaListenerContainerFactory for connection: {}", name);
            return factory;
        });
    }

    private void applyListenerProperties(ConcurrentKafkaListenerContainerFactory<?, ?> factory,
                                         KafkaProperties.Listener listener, String connectionName) {

        // Batch listener configuration
        factory.setBatchListener(listener.getType() == KafkaProperties.Listener.Type.BATCH);

        // Ack mode configuration
        if (listener.getAckMode() != null) {
            factory.getContainerProperties().setAckMode(listener.getAckMode());
            log.debug("Configured ack-mode: {} for connection: {}",
                    listener.getAckMode(), connectionName);
        }

        // Concurrency configuration
        if (listener.getConcurrency() != null) {
            factory.setConcurrency(listener.getConcurrency());
            log.debug("Configured concurrency: {} for connection: {}",
                    listener.getConcurrency(), connectionName);
        }

        // Poll timeout configuration
        if (listener.getPollTimeout() != null) {
            factory.getContainerProperties().setPollTimeout(
                    listener.getPollTimeout().toMillis());
            log.debug("Configured poll timeout: {} for connection: {}",
                    listener.getPollTimeout(), connectionName);
        }

        // No poll threshold configuration
        if (listener.getNoPollThreshold() != null) {
            factory.getContainerProperties().setNoPollThreshold(
                    listener.getNoPollThreshold());
            log.debug("Configured no-poll threshold: {} for connection: {}",
                    listener.getNoPollThreshold(), connectionName);
        }

        // Ack count configuration (for COUNT and COUNT_TIME ack modes)
        if (listener.getAckCount() != null) {
            factory.getContainerProperties().setAckCount(listener.getAckCount());
            log.debug("Configured ack count: {} for connection: {}",
                    listener.getAckCount(), connectionName);
        }

        // Ack time configuration (for TIME and COUNT_TIME ack modes)
        if (listener.getAckTime() != null) {
            factory.getContainerProperties().setAckTime(
                    listener.getAckTime().toMillis());
            log.debug("Configured ack time: {} for connection: {}",
                    listener.getAckTime(), connectionName);
        }

        // Idle event interval configuration
        if (listener.getIdleEventInterval() != null) {
            factory.getContainerProperties().setIdleEventInterval(
                    listener.getIdleEventInterval().toMillis());
            log.debug("Configured idle event interval: {} for connection: {}",
                    listener.getIdleEventInterval(), connectionName);
        }

        // Idle partition event interval configuration
        if (listener.getIdlePartitionEventInterval() != null) {
            factory.getContainerProperties().setIdlePartitionEventInterval(
                    listener.getIdlePartitionEventInterval().toMillis());
            log.debug("Configured idle partition event interval: {} for connection: {}",
                    listener.getIdlePartitionEventInterval(), connectionName);
        }

        // Monitor interval configuration
        if (listener.getMonitorInterval() != null) {
            factory.getContainerProperties().setMonitorInterval(
                    (int) listener.getMonitorInterval().getSeconds());
            log.debug("Configured monitor interval: {} for connection: {}",
                    listener.getMonitorInterval(), connectionName);
        }

        // Log container config configuration
        if (listener.getLogContainerConfig() != null) {
            factory.getContainerProperties().setLogContainerConfig(
                    listener.getLogContainerConfig());
            log.debug("Configured log container config: {} for connection: {}",
                    listener.getLogContainerConfig(), connectionName);
        }

        // Missing topics fatal configuration
        factory.getContainerProperties().setMissingTopicsFatal(
                listener.isMissingTopicsFatal());
        log.debug("Configured missing topics fatal: {} for connection: {}",
                listener.isMissingTopicsFatal(), connectionName);

        // Immediately stop on error configuration
        factory.getContainerProperties().setStopImmediate(
                listener.isImmediateStop());
        log.debug("Configured immediate stop: {} for connection: {}",
                listener.isImmediateStop(), connectionName);

        // Observation enabled configuration (Micrometer Observability - Spring Boot 3.x+)
        factory.getContainerProperties().setObservationEnabled(
                listener.isObservationEnabled());
        log.debug("Configured observation enabled: {} for connection: {}",
                listener.isObservationEnabled(), connectionName);

        // Async acks configuration (Spring Boot 3.x+)
        if (listener.getAsyncAcks() != null) {
            factory.getContainerProperties().setAsyncAcks(listener.getAsyncAcks());
            log.debug("Configured async acks: {} for connection: {}",
                    listener.getAsyncAcks(), connectionName);
        }

        // Client ID configuration
        if (listener.getClientId() != null && !listener.getClientId().isEmpty()) {
            factory.getContainerProperties().setClientId(listener.getClientId());
            log.debug("Configured client ID: {} for connection: {}",
                    listener.getClientId(), connectionName);
        }

        log.info("Applied all listener properties for connection: {}", connectionName);
    }


    private CommonErrorHandler kafkaCommonErrorHandler() {

        final ConsumerRecordRecoverer recoverer = (ConsumerRecord<?, ?> record, Exception exception)
                -> log.error("KAFKA_ERROR_HANDLER:: Skipping non-retryable record topic={}, partition={}, offset={}, key={}, error={}",
                record.topic(), record.partition(), record.offset(), record.key(), exception.getMessage(), exception);

        // Long.MAX_VALUE attempts => transient failures are retried indefinitely (never skipped/committed).
        final DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(15000, Long.MAX_VALUE));

        handler.addNotRetryableExceptions(DeserializationException.class);
        handler.setLogLevel(KafkaException.Level.WARN);
        log.info("Configured Kafka CommonErrorHandler: transient failures retried indefinitely every {}ms; " + "bad-data failures logged and skipped", 15000);
        return handler;
    }

    private <T> BatchInterceptor<String, T> batchInterceptor() {
        return (ConsumerRecords<String, T> records, Consumer<String, T> consumer) -> {
            try {
                MDC.clear();
                String traceId;
                if (!records.isEmpty()) {
                    ConsumerRecord<String, T> firstRecord = records.iterator().next();
                    if (firstRecord.headers().lastHeader(Constants.BATCH_ID) != null) {
                        traceId = new String(firstRecord.headers().lastHeader(Constants.BATCH_ID).value(), StandardCharsets.UTF_8) + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
                    } else {
                        traceId = UUID.randomUUID().toString();
                    }
                    MDC.put(Constants.BATCH_ID, traceId);
                }
                String hostName = InetAddress.getLocalHost().getHostName();
                log.info("Consumed batch of {} records on host: {}", records.count(), hostName);
            } catch (Exception e) {
                log.warn("Error adding batchId in header, adding new traceId: {}", e.getMessage(), e);
                MDC.put(Constants.BATCH_ID, UUID.randomUUID().toString());
            }
            return records;
        };
    }
}
