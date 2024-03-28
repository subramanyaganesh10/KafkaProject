package com.kafka.KafkaProject.services;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.kafka.KafkaProject.Model.ModelWorker;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class KafkaListenerMain {

    public final List<String> googleListener = new ArrayList<>();
    public final List<String> netflixListener = new ArrayList<>();
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    @KafkaListener(
            topics = "${kafka.topic.google}",
            groupId = "groupid"
            //For consumers, the group.id property is essential. It identifies the consumer group to which a consumer belongs.
            // Kafka uses consumer groups to manage partition assignment and offset tracking.
            //Producers publish messages to Kafka topics without regard to consumer groups or partition assignment.
            // They simply send messages to the specified topic(s) without any coordination with other producers or consumers.
    )
    public void googleListener(String message) {

        // This method will be invoked when a message is received from the "myFirstTopic" Kafka topic.
        System.out.println("Listener 1:::Received message: " + gson.fromJson(message, ModelWorker.class).getValues());
        // Further processing of the received message can be done here.
        googleListener.add(message);
    }

    @KafkaListener(
            topics = "${kafka.topic.netflix}",
            groupId = "groupid"
            //For consumers, the group.id property is essential. It identifies the consumer group to which a consumer belongs.
            // Kafka uses consumer groups to manage partition assignment and offset tracking.
            //Producers publish messages to Kafka topics without regard to consumer groups or partition assignment.
            // They simply send messages to the specified topic(s) without any coordination with other producers or consumers.
    )
    public void netflixListener(String message) {

        // This method will be invoked when a message is received from the "myFirstTopic" Kafka topic.
        System.out.println("Listener 2:::Received message: " + gson.fromJson(message, ModelWorker.class).getValues());
        // Further processing of the received message can be done here.
        netflixListener.add(message);
    }

}
