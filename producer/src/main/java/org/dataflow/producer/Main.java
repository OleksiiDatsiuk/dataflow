package org.dataflow.producer;

import org.dataflow.producer.client.ProducerClient;

import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        ProducerClient producerClient = ProducerClient.createProducer("localhost", 2023);
        producerClient.sendMessage("""
                {
                  "nodeId": "123e4567-e89b-12d3-a456-426614174000",
                  "connectionType": "PRODUCER",
                  "requestType": "INITIAL_REQUEST",
                  "message": "Hello, this is my initial request."
                }
                """);
        producerClient.sendMessage("""
                {
                  "nodeId": "123e4567-e89b-12d3-a456-426614174000",
                  "connectionType": "PRODUCER",
                  "requestType": "BASIC_REQUEST",
                  "message": "{ \\"topic\\": \\"some-topic-1\\", \\"message\\": \\"This is shit message.\\" }"
                }
                """);
        TimeUnit.SECONDS.sleep(20);
        producerClient.sendMessage("""
                {
                  "nodeId": "123e4567-e89b-12d3-a456-426614174000",
                  "connectionType": "PRODUCER",
                  "requestType": "BASIC_REQUEST",
                  "message": "{ \\"topic\\": \\"some-topic-1\\", \\"message\\": \\"This is shit message.\\" }"
                }
                """);
    }

}