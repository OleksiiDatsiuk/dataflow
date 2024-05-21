package org.dataflow.producer;

import org.dataflow.producer.client.ProducerClient;

import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        ProducerClient producerClient = new ProducerClient("localhost", 2023);
        TimeUnit.SECONDS.sleep(5);
        producerClient.sendMessage("some-topic-1", "This is shit message");
        TimeUnit.SECONDS.sleep(5);
        producerClient.sendMessage("some-topic-1", "T222222his is shit message");
        producerClient.close();
    }

}