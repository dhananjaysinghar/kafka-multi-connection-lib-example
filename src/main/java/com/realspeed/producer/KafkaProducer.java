package com.realspeed.producer;

import com.realspeed.config.kafka.support.KafkaTemplateFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
public class KafkaProducer {

    @Value("${app.kafka.topics.test-topic}")
    private String topicName;

    @Autowired
    private KafkaTemplateFactory kafkaTemplateFactory;

    public void sendMessage(String message) {
        KafkaTemplate<String, String> template = kafkaTemplateFactory.getStringTemplate("string-key-value");

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, UUID.randomUUID().toString(), message);
        Headers headers = record.headers();
        headers.add(new RecordHeader("trace-id", UUID.randomUUID().toString().getBytes()));

        template.send(record)
                .thenAccept(result -> {
                   log.info("Message sent successfully to topic: {} partition: {}, offset: {}",
                           result.getRecordMetadata().topic(), result.getRecordMetadata().partition(),
                           result.getRecordMetadata().offset() );
                })
                .exceptionally(ex -> {
                    log.error("Failed to send message: {}", ex.getMessage());
                    return null;
                });

    }

}
