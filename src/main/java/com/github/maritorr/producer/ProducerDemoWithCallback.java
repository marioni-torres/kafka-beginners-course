package com.github.maritorr.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallback {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", String.format("Hello World %d!", i));

            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        LOGGER.info("Received metadata. Topic {}, partition {}, offset {}, timestamp {}", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                    } else {
                        LOGGER.error("Error while producing message", exception);
                    }
                }
            }).get();
        }
        // flush data
        producer.flush();
        producer.close();
    }
}
