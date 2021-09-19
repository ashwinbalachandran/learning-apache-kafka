package com.learning.kafka.course;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroupsSeek {
    public static void main(String[] args) {
        Logger logger  = LoggerFactory.getLogger(ConsumerDemoGroupsSeek.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic_2";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to topic
//        consumer.subscribe(Collections.singleton(topic));

        //assign and seek used to replay data or fetch a specific message
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seeking
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int maxMessages = 5;
        boolean continueReading = true;
        int messagesRead = 0;
        //poll for new data
        while(continueReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord record: records) {
                messagesRead++;
                logger.info("Key = "+ record.key() + "Value =" + record.value());
                logger.info("Partition = "+ record.partition() + "Offset =" + record.offset());
                if(messagesRead >= maxMessages) {
                    continueReading = false;
                    break;
                }
            }

        }
        logger.info("End of the line brother");
    }
}
