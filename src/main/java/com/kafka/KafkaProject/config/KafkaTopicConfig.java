package com.kafka.KafkaProject.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    public NewTopic topic(String topic) {
        int partitions = 5;
        short replicationFactor = 1;
        return new NewTopic(topic, partitions, replicationFactor);
    }

}
