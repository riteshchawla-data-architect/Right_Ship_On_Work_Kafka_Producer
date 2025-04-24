package com.spark.streaming.kafka.producer.services;

import com.spark.streaming.kafka.producer.models.FiservUniversalDataModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class FiservKafkaProducerService {

    @Autowired
    @Qualifier("stringKafkaTemplate")
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    @Qualifier("customKafkaTemplate")
    private KafkaTemplate<String, FiservUniversalDataModel> customModelKafkaTemplate;


    @Value(value = "${spring.kafka.publish.topic.name}")
    private String topicName;

    public void sendStringMessage(String message) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });
    }

    public void sendCustomModelMessage(FiservUniversalDataModel modelMessage) {
        CompletableFuture<SendResult<String, FiservUniversalDataModel>> future = customModelKafkaTemplate.send(topicName, modelMessage);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + modelMessage.toString() +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        modelMessage.toString() + "] due to : " + ex.getMessage());
            }
        });
    }
}
