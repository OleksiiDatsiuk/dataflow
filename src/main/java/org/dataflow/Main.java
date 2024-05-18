package org.dataflow;


import lombok.SneakyThrows;
import org.dataflow.broker.AbstractBroker;
import org.dataflow.broker.ManagedBroker;

import java.util.concurrent.TimeUnit;

public class Main {

    @SneakyThrows
    public static void main(String[] args) {
        AbstractBroker broker = ManagedBroker.getInstance(2023, "Broker1");
        broker.run();
    }

}