package com.spark.streaming.kafka.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class FiservKafkaProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(FiservKafkaProducerApplication.class, args);
    }

}
