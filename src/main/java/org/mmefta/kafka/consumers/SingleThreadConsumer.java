package org.mmefta.kafka.service;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.mmefta.kafka.config.KafkaConfig;
import org.mmefta.kafka.dto.ClickEvent;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerMain {

    public static void main(String[] args) {
        Properties properties = KafkaConfig.buildConsumerProps(
                "ecommerce.events.group.single",
                true,
                false
        );
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        AtomicInteger consumePartitionMsgCount = new AtomicInteger(0);


        try (ConsumerResource<String, ClickEvent> consumerResource = new ConsumerResource<>(
                1,
                KafkaConfig.EVENTS_TOPIC,
                properties,
                false, // consumerParallel
                consumePartitionMsgCount)) {
            // Your logic for consuming messages goes here

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
