package org.dataflow;


import lombok.SneakyThrows;
import org.dataflow.broker.AbstractBroker;
import org.dataflow.broker.ManagedBroker;

import java.util.concurrent.TimeUnit;

public class Main {

    @SneakyThrows
    public static void main(String[] args) {
        AbstractBroker broker = ManagedBroker.getInstance(123, "Broker1");
        broker.run();
        TimeUnit.SECONDS.sleep(20);
        broker.shutDown();
    }

}