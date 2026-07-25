package com.realspeed.config.kafka.support;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration properties for managing multiple Kafka connections.
 *
 * This class enables the application to connect to multiple Kafka clusters simultaneously, each with its
 * own set of configuration properties. It binds properties from the configuration file with the prefix "app.kafka".
 *
 * Configuration Structure
 *
 * app:
 *   kafka:
 *     connections:
 *       connectionName1:
 *         bootstrap-servers: server1:9092,server2:9092
 *         consumer:
 *           key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
 *           value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
 *           auto-offset-reset: earliest
 *         producer:
 *           key-serializer: org.apache.kafka.common.serialization.StringSerializer
 *           value-serializer: org.apache.kafka.common.serialization.StringSerializer
 *         properties:
 *           security.protocol: SASL_SSL
 *
 *       connectionName2:
 *         bootstrap-servers: localhost:9092
 *         consumer:
 *           auto-offset-reset: latest
 *
 * Usage Example
 *
 * Once configured, the connection names can be used to reference specific Kafka templates and listener factories in your application:
 *
 * @KafkaListener(
 *     topics = "topic-name",
 *     containerFactory = "#{kafkaListenerFactories[connection-1]}"
 * )
 * public void listen(String message) {
 *     // Process message from connection-1
 * }
 */
@ConfigurationProperties(prefix = "app.kafka")
@Getter
@Setter
public class KafkaConnectionProperties {


    private Map<String, KafkaProperties> connections = new HashMap<>();

}
