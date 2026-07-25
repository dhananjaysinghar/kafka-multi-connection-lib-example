package com.realspeed.config.kafka.support;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class KafkaTemplateFactory {

    private KafkaConnectionProperties properties;
    private Map<String, KafkaTemplate<String, String>> stringTemplates = new ConcurrentHashMap<>();
    private Map<String, KafkaTemplate<String, Object>> objectTemplates = new ConcurrentHashMap<>();

    public KafkaTemplateFactory(KafkaConnectionProperties properties) {
        this.properties = properties;
    }



    public KafkaTemplate<String, String> getStringTemplate(String connectionName) {
        return stringTemplates.computeIfAbsent(connectionName, (String name) -> {
            KafkaProperties cfg = properties.getConnections().get(name);
            if (cfg == null) {
                throw new IllegalArgumentException("No Kafka connection configured with name: " + name);
            }

            Map<String, Object> producerProps = cfg.buildProducerProperties();
            Object valueSerializer = producerProps.get("value.serializer");

            if (valueSerializer == null || !valueSerializer.toString().contains("StringSerializer")) {
                throw new IllegalStateException(
                        "Connection '" + name + "' is not configured for String serialization. " +
                                "Current serializer: " + valueSerializer
                );
            }

            DefaultKafkaProducerFactory<String, String> factory =
                    new DefaultKafkaProducerFactory<>(producerProps);

            KafkaTemplate<String, String> template = new KafkaTemplate<>(factory);

            // Apply template-specific properties from KafkaProperties.Template
            applyTemplateProperties(template, cfg.getTemplate(), name);

            log.info("Lazily created String KafkaTemplate for connection: {}", name);
            return template;
        });
    }

    public KafkaTemplate<String, Object> getObjectTemplate(String connectionName) {
        return objectTemplates.computeIfAbsent(connectionName, (String name) -> {
            KafkaProperties cfg = properties.getConnections().get(name);
            if (cfg == null) {
                throw new IllegalArgumentException("No Kafka connection configured with name: " + name);
            }

            Map<String, Object> producerProps = cfg.buildProducerProperties();
            Object valueSerializer = producerProps.get("value.serializer");

            if (valueSerializer == null || valueSerializer.toString().contains("StringSerializer")) {
                throw new IllegalStateException(
                        "Connection '" + name + "' is configured for String, not Object serialization. " +
                                "Use getStringTemplate() instead."
                );
            }

            DefaultKafkaProducerFactory<String, Object> factory =
                    new DefaultKafkaProducerFactory<>(producerProps);

            KafkaTemplate<String, Object> template = new KafkaTemplate<>(factory);

            // Apply template-specific properties from KafkaProperties.Template
            applyTemplateProperties(template, cfg.getTemplate(), name);

            log.info("Lazily created Object KafkaTemplate for connection: {}", name);
            return template;
        });
    }

    private void applyTemplateProperties(KafkaTemplate<?, ?> template, KafkaProperties.Template templateProps, String connectionName) {

        // Default topic configuration
        if (templateProps.getDefaultTopic() != null
                && !templateProps.getDefaultTopic().isEmpty()) {
            template.setDefaultTopic(templateProps.getDefaultTopic());
            log.debug("Configured default topic: {} for connection: {}",
                    templateProps.getDefaultTopic(), connectionName);
        }

        // Transaction ID prefix configuration
        if (templateProps.getTransactionIdPrefix() != null && !templateProps.getTransactionIdPrefix().isEmpty()) {
            template.setTransactionIdPrefix(templateProps.getTransactionIdPrefix());
            log.debug("Configured transaction ID prefix: {} for connection: {}",
                    templateProps.getTransactionIdPrefix(), connectionName);
        }

        // Observation enabled configuration (Micrometer Observability - Spring Boot 3.x+)
        template.setObservationEnabled(templateProps.isObservationEnabled());
        log.debug("Configured observation enabled: {} for connection: {}",
                templateProps.isObservationEnabled(), connectionName);

        log.debug("Applied all template properties for connection: {}", connectionName);
    }

    /**
     * Destroys all created KafkaTemplate instances and releases resources.
     * Called automatically during application shutdown.
     */
    @PreDestroy
    public void destroy() {
        log.info("Destroying {} String KafkaTemplates and {} Object KafkaTemplates",
                stringTemplates.size(), objectTemplates.size());

        stringTemplates.values().forEach(KafkaTemplate::destroy);
        objectTemplates.values().forEach(KafkaTemplate::destroy);

        stringTemplates.clear();
        objectTemplates.clear();
    }
}
