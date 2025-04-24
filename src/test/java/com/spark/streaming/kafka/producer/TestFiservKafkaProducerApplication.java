package com.spark.streaming.kafka.producer;

import org.springframework.boot.SpringApplication;

public class TestFiservKafkaProducerApplication {

    public static void main(String[] args) {
        SpringApplication.from(FiservKafkaProducerApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
