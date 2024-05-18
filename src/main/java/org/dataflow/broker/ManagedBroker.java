package org.dataflow.broker;

import lombok.extern.slf4j.Slf4j;
import org.dataflow.connection.ConnectionManager;
import org.dataflow.data.serializer.Serializer;

@Slf4j
public class ManagedBroker extends AbstractBroker{

    private static ManagedBroker INSTANCE;

    private final ConnectionManager connectionManager;

    private ManagedBroker(int port, String name) {
        super(port, name);
        connectionManager = new ConnectionManager(new Serializer());
    }

    public static ManagedBroker getInstance(int port, String name) {
        if (INSTANCE == null) {
            INSTANCE = new ManagedBroker(port, name);
        }

        return INSTANCE;
    }

    public static ManagedBroker getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("Broker has not been initialized yet!");
        }

        return INSTANCE;
    }

    @Override
    public void run() {
        this.start();
        log.info("Server successfully started on address {}:{}", socket.getInetAddress().getHostAddress(),
                socket.getLocalPort());

        this.processRequests(connectionManager::processConnection);
    }

}
