package com.spark.streaming.kafka.producer.schedulars;

import com.spark.streaming.kafka.producer.services.FiservKafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KafkaMessageProducerSchedular {

    @Autowired
    private FiservKafkaProducerService kafkaProducerService;


}
