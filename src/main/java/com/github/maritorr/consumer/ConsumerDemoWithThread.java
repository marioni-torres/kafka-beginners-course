package com.github.maritorr.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "my-sixth-application";
    private static final String FIRST_TOPIC = "first_topic";

    public static void main(String[] args) {

        // Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to topic(s)
        consumer.subscribe(Collections.singleton(FIRST_TOPIC));

        // poll for new data - with Thread
        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumerRunnable = new ConsumerRunnable(consumer, latch);
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        LOG.info("Started consumer thread {} - {}", consumerThread.getId(), consumerThread.getName());

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            LOG.info("Caught shutdown hook");
            ((ConsumerRunnable)consumerRunnable).shutdown();

            try {
                LOG.info("Latch count at shutdown {}", latch.getCount());
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOG.info("Application has exited");
        }));

        try {
            LOG.info("Latch count at start thread {}", latch.getCount());
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Application got interrupted", e);
        } finally {
            LOG.info("Application is closing...");
        }
    }

    private static class ConsumerRunnable implements Runnable {

        private static final Logger LOG = LoggerFactory.getLogger(ConsumerRunnable.class);

        private KafkaConsumer<String, String> consumer;
        private CountDownLatch latch;

        private ConsumerRunnable(KafkaConsumer<String, String> consumer, CountDownLatch latch) {
            this.consumer = consumer;
            this.latch = latch;
        }

        @Override
        public void run() {
            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        LOG.info("Key {}, Value {}", record.key(), record.value());
                        LOG.info("Partition {}, Offset {}", record.partition(), record.offset());
                    }
                }
            } catch (WakeupException we) {
                LOG.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
                LOG.info("Latch count {} at shutdown signal", latch.getCount());
            }
        }

        public void shutdown(){
            consumer.wakeup();
        }
    }
}
