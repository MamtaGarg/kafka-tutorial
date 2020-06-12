package com.kafka.tutorial.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;

public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application";

        //Create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList("first_topic"));

        // to subscribe single topic
        // consumer.subscribe(Collections.singleton("first_topic"));

        // poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0
            for(ConsumerRecord record : records) {
                logger.info("Key : " + record.key() + ", Value : " + record.value());
                logger.info("Partition : " + record.partition() + ", Offset : " + record.offset());
            }
        }
    }
}
