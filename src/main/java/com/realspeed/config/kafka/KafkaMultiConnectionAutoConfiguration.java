package com.realspeed.config.kafka;

import com.realspeed.config.kafka.support.KafkaConnectionProperties;
import com.realspeed.config.kafka.support.KafkaListenerContainerFactoryManager;
import com.realspeed.config.kafka.support.KafkaTemplateFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
//@AutoConfiguration
@EnableConfigurationProperties(KafkaConnectionProperties.class)
@Configuration
public class KafkaMultiConnectionAutoConfiguration {

    @Bean
    public KafkaTemplateFactory kafkaTemplateFactory(KafkaConnectionProperties kafkaConnectionProperties) {
        log.info("KafkaMultiConnectionAutoConfiguration initialized with properties: {}", kafkaConnectionProperties);
        return new KafkaTemplateFactory(kafkaConnectionProperties);
    }

    @Bean
    public KafkaListenerContainerFactoryManager kafkaListenerContainerFactoryManager(KafkaConnectionProperties kafkaConnectionProperties) {
        log.info("Registered KafkaListenerContainerFactoryManager for lazy initialization of KafkaListenerContainerFactories with properties: {}", kafkaConnectionProperties);
        return new KafkaListenerContainerFactoryManager(kafkaConnectionProperties);
    }
}
