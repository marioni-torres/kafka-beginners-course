package com.github.maritorr.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String FIRST_TOPIC = "first_topic";

    public static void main(String[] args) {

        // Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(FIRST_TOPIC, 0);
        consumer.assign(Collections.singleton(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, 10L);

        int totalMessagesToRead = 5;
        int messagesRead = 0;

        // poll for new data
        while(messagesRead <= totalMessagesToRead) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                messagesRead++;
                LOG.info("Key {}, Value {}", record.key(), record.value());
                LOG.info("Partition {}, Offset {}", record.partition(), record.offset());

                if (messagesRead >= totalMessagesToRead) {
                    break;
                }
            }
        }
    }
}
