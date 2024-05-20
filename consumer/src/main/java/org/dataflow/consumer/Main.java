package org.dataflow.consumer;

import org.dataflow.consumer.client.ConsumerClient;

public class Main {
    public static void main(String[] args) {
        ConsumerClient consumerClient = ConsumerClient.createConsumer("localhost", 9999, "some-topic");

        consumerClient.connect();
        consumerClient.consume(System.out::println);
        consumerClient.disconnect();
    }
}