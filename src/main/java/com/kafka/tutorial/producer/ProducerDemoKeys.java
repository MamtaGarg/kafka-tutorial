package com.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int index = 0; index < 10; index++) {
            // Create a producer record
            String topic = "first_topic";
            String value = "Hello World " + Integer.toString(index);
            String key = "id_" + Integer.toString(index);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key : " + key); //log the key
            // If we run this code multiple times, every time it will store the same key to same partition.
            // id_0 is going to partition 1
            // id_1 is going to partition 0
            // id_2 is going to partition 2
            // id_3 is going to partition 0
            // id_4 is going to partition 2
            // id_5 is going to partition 2
            // id_6 is going to partition 0
            // id_7 is going to partition 2
            // id_8 is going to partition 1
            // id_9 is going to partition 2

            //Send the data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if (exception == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic : " + metadata.topic() + "\n" +
                                "Partition : " + metadata.partition() + "\n" +
                                "Offset : " + metadata.offset() + "\n" +
                                "Timestamp : " + metadata.timestamp());
                    } else {
                        logger.error("Error while producing :", exception);
                    }
                }
            }).get(); //block the send() to make it synchronous - don't do this in production!
        }

        //flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
