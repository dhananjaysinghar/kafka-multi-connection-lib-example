# kafka-multi-connection-lib-example
kafka-multi-connection-lib-example

~~~ yaml
spring:
  application:
    name: kafka-producer-consumer-lib

app:
  kafka:
    topics:
      test-topic: ${KAFKA_TOPIC_NAME:TEST-TOPIC}
    connections:
      string-key-value:
        bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
        consumer:
          group-id: ${KAFKA_TOPIC_CONSUMER_GROUP:test-consumer-group}
          enable-auto-commit: false
          auto-offset-reset: earliest
          max-poll-records: ${KAFKA_CONSUMER_MAX_POLL_RECORDS:200}
          key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
          value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
          properties:
            max.poll.interval.ms: ${KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS:2700000}
            max.partition.fetch.bytes: 10485760
            fetch.max.bytes: 104857600
            fetch.min.bytes: 1024
            fetch.max.wait.ms: 5000
            session.timeout.ms: 180000
            heartbeat.interval.ms: 60000
        listener:
          ack-mode: MANUAL
          type: BATCH
          concurrency: ${KAFKA_CONSUMER_CONCURRENCY:4}
        producer:
          key-serializer: org.apache.kafka.common.serialization.StringSerializer
          value-serializer: org.apache.kafka.common.serialization.StringSerializer
          batch-size: 524288
          buffer-memory: 33554432
          compression-type: lz4
          properties:
            linger.ms: 10
            max.block.ms: 180000
            request.timeout.ms: 180000
            reconnect.backoff.max.ms: 10000
            retry.backoff.ms: 5000
            retry.backoff.max.ms: 10000
            enable.idempotence: true
        properties:
          security.protocol: ${KAFKA_SECURITY_PROTOCOL:PLAINTEXT}
~~~
