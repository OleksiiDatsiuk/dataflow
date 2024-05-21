package org.dataflow.broker;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dataflow.connection.ConnectionManager;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ManagedBroker extends AbstractBroker{

    private static ManagedBroker INSTANCE;

    private final ConnectionManager connectionManager;

    @Getter
    protected final Map<Socket, Map<String, Long>> consumerOffsets = new HashMap<>();

    @Getter
    protected final Map<UUID, Set<String>> acknowledgments = new ConcurrentHashMap<>();

    private ManagedBroker(int port, String name) {
        super(port, name);
        connectionManager = new ConnectionManager();
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
