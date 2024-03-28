package com.kafka.KafkaProject.services;

import com.kafka.KafkaProject.config.KafkaProducerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;

@Service
public class KafkaDeleteData {

    @Autowired
    KafkaProducerConfig kafkaProducerConfig;

    public ResponseEntity<String> deleteTopic(String topicName) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerConfig.bootstrapServer);

        try (AdminClient adminClient = AdminClient.create(props)) {
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            KafkaFuture<Void> all = deleteTopicsResult.all();
            all.get(); // Wait for the deletion to complete
            return new ResponseEntity<>("Topic " + topicName + " deleted successfully.",HttpStatus.ACCEPTED);
        } catch (Exception e){
            return new ResponseEntity<>(e.getLocalizedMessage(),HttpStatus.NOT_ACCEPTABLE);
        }
    }
}
