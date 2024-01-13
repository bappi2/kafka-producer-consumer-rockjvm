package org.mmefta.kafka.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.mmefta.kafka.config.KafkaConfig;
import org.mmefta.kafka.dto.ClickEvent;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class PerPartitionConsumer {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Properties properties = KafkaConfig.buildConsumerProps(
				"ecommerce.events.group",
				true,
				false
		);

		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		try (AdminClient adminClient = AdminClient.create(properties)) {
			DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(List.of(KafkaConfig.EVENTS_TOPIC));
			TopicDescription topicDescription = describeTopicsResult.values().get(KafkaConfig.EVENTS_TOPIC).get();

			int partitionNum = topicDescription.partitions().size();

			AtomicInteger consumePartitionMsgCount = new AtomicInteger(0);

			for (int id = 0; id < partitionNum; id++) {
				launchConsumer(id, KafkaConfig.EVENTS_TOPIC, properties, consumePartitionMsgCount);
			}
		}
	}

	private static void launchConsumer(int id, String topic, Properties properties,
									   AtomicInteger consumePartitionMsgCount) {
		new Thread(() -> {
			try (ConsumerResource<String, ClickEvent> consumerResource = new ConsumerResource<>(
					id, topic, properties, false, consumePartitionMsgCount
			)) {
				// Your logic for consuming messages goes here
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();
	}
}
