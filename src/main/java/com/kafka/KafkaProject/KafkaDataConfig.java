package com.kafka.KafkaProject;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.kafka.KafkaProject.Model.ModelWorker;
import com.kafka.KafkaProject.config.KafkaTopicConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;

@Configuration
public class KafkaDataConfig {

    @Autowired
    KafkaTopicConfig kafkaTopicConfig;
    @Value("${kafka.topic.google}")
    String google;

    @Value("${kafka.topic.netflix}")
    String netflix;

    @Bean
    CommandLineRunner commandLineRunner(KafkaTemplate<String, String> kafkaTemplate) {
        return args -> {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            try (BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/GoogleStock Price.csv"))) {
                reader.lines()
                        .skip(1) // Skip header
                        .map(line -> gson.toJson(new ModelWorker(line.split(",")[0], line.split(",")[2])))
                        .forEach(a -> kafkaTemplate.send(google, a));
                //kafkaTopicConfig.topic(google).name()
            } catch (Exception e) {
                // Handle exceptions
                e.printStackTrace();
            }


            try (BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/NFLX.csv"))) {
                reader.lines()
                        .skip(1) // Skip header
                        .map(line -> gson.toJson(new ModelWorker(line.split(",")[0], line.split(",")[2])))
                        .forEach(a -> kafkaTemplate.send(netflix, a));
                //kafkaTopicConfig.topic(netflix).name()
            } catch (Exception e) {
                // Handle exceptions
                e.printStackTrace();
            }
        };
    }
}
