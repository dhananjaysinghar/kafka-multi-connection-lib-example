package com.realspeed.controller;

import com.realspeed.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private KafkaProducer kafkaProducer;

    @GetMapping
    public void test() {
        String message = "Test message:"+ System.currentTimeMillis();
        log.info("Sending test message: {}", message);
        kafkaProducer.sendMessage(message);
    }
}
