package com.spark.streaming.kafka.producer;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

//@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

    //@Bean
    //ServiceConnection
    KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
    }

}
