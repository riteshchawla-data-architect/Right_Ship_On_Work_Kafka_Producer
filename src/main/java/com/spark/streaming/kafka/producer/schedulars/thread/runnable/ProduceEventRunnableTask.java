package com.spark.streaming.kafka.producer.schedulars.thread.runnable;

import com.spark.streaming.kafka.producer.constant.SendMessageType;
import com.spark.streaming.kafka.producer.models.FiservUniversalDataModel;
import com.spark.streaming.kafka.producer.services.FiservKafkaProducerService;

import java.util.Date;
import java.util.List;

public class ProduceEventRunnableTask {

    private static FiservKafkaProducerService kafkaProducerService;

    private TaskRunner taskRunner;

    public static class TaskRunner implements Runnable {

        private String jsonData;

        private List<String> requestData;

        private FiservUniversalDataModel requestDataObject;

        private List<FiservUniversalDataModel> requestDataObjectList;

        private SendMessageType sendMessageType;

        @Override
        public void run() {
            System.out.println(new Date()+" Runnable Task with "+ sendMessageType
                    +" on thread "+Thread.currentThread().getName());
            if(sendMessageType == null || sendMessageType.name().equals(SendMessageType.SINGLE_STRING_MESSAGE)) {
                kafkaProducerService.sendStringMessage(jsonData);
            } else if(sendMessageType.name().equals(SendMessageType.MULTI_STRING_MESSAGE)) {
                requestData.forEach(data -> kafkaProducerService.sendStringMessage(data));
            } else if(sendMessageType.name().equals(SendMessageType.SINGLE_CUSTOM_MODEL_MESSAGE)) {
                kafkaProducerService.sendCustomModelMessage(requestDataObject);
            } else if(sendMessageType.name().equals(SendMessageType.MULTI_CUSTOM_MODEL_MESSAGE)) {
                requestDataObjectList.forEach(dataObject -> kafkaProducerService.sendCustomModelMessage(dataObject));
            }
        }

        public String getJsonData() {
            return jsonData;
        }

        public void setJsonData(String jsonData) {
            this.jsonData = jsonData;
        }

        public List<String> getRequestData() {
            return requestData;
        }

        public void setRequestData(List<String> requestData) {
            this.requestData = requestData;
        }

        public FiservUniversalDataModel getRequestDataObject() {
            return requestDataObject;
        }

        public void setRequestDataObject(FiservUniversalDataModel requestDataObject) {
            this.requestDataObject = requestDataObject;
        }

        public List<FiservUniversalDataModel> getRequestDataObjectList() {
            return requestDataObjectList;
        }

        public void setRequestDataObjectList(List<FiservUniversalDataModel> requestDataObjectList) {
            this.requestDataObjectList = requestDataObjectList;
        }

        public SendMessageType getSendMessageType() {
            return sendMessageType;
        }

        public void setSendMessageType(SendMessageType sendMessageType) {
            this.sendMessageType = sendMessageType;
        }
    }

    public TaskRunner getTaskRunner() {
        return taskRunner;
    }

    public void setTaskRunner(TaskRunner taskRunner) {
        this.taskRunner = taskRunner;
    }

    public FiservKafkaProducerService getKafkaProducerService() {
        return kafkaProducerService;
    }

    public void setKafkaProducerService(FiservKafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }
}
