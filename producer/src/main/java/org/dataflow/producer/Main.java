package org.dataflow.producer;

import org.dataflow.producer.client.ProducerClient;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        ProducerClient producerClient = new ProducerClient("localhost", 2023);
        int counter = 0;
        while (true) {
            producerClient.sendMessage("some-topic-1", "message-%s".formatted(counter));

            if (counter++ == 8) {
                break;
            }
        }

        producerClient.close();
    }

}