package com.realspeed.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class KafkaConsumer {

    @Value("${app.kafka.topics.test-topic}")
    private String topicName;

    @KafkaListener(
            topics = "${app.kafka.topics.test-topic}",
            containerFactory = "#{kafkaListenerContainerFactoryManager.getStringFactory('string-key-value')}",
            groupId = "${app.kafka.connections.string-key-value.consumer.group-id}"
    )
    public void consume(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        log.info("Received message from topic {}, size:{}", topicName, records.size());
        try{
            for (ConsumerRecord<String, String> record : records) {
                log.info("Received message from topic {}, partition {}, offset {}, key {}, value {}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Error processing records from topic {}: {}", topicName, e.getMessage(), e);
            throw e; // Rethrow the exception to trigger retry or dead-letter handling
        }
    }
}
