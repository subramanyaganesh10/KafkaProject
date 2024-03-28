package com.kafka.KafkaProject;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProjectApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaProjectApplication.class, args);
    }

    //my main producer
//	@Bean
//	CommandLineRunner commandLineRunner(KafkaTemplate<String, String> kafkaTemplate) {
//		return args -> {
//			for (int i = 0; i < 10000000; i++) {
//				kafkaTemplate.send("myFirstTopic","hello buddy :) "+i);
//			}
//		};
//	}

}
