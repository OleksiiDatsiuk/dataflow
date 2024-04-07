package org.dataflow.connection;

import lombok.extern.slf4j.Slf4j;
import org.dataflow.common.ConnectionType;
import org.dataflow.model.BrokerConnection;
import org.dataflow.model.NodeRequest;
import org.dataflow.util.SocketCommunicator;

import java.net.Socket;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BrokerConnections {

    private static BrokerConnections INSTANCE;

    private final ConcurrentHashMap<UUID, BrokerConnection> brokerConnections = new ConcurrentHashMap<>();

    public void registerConnection(NodeRequest nodeRequest, Socket socket) {
        ConnectionType connectionType = nodeRequest.connectionType();

        if (connectionType == ConnectionType.METADATA_MANAGER && containsMetadataManagerConnection()) {
            return;
        }

        brokerConnections.put(nodeRequest.nodeId(), new BrokerConnection(connectionType, socket));
        SocketCommunicator.sendMessage(socket, "Connection registered successfully!");

        log.info("Registered connection of type {} with address {}", connectionType, socket.getInetAddress().getHostAddress());
    }

    public BrokerConnection getConnection(UUID nodeId) {
        return brokerConnections.get(nodeId);
    }

    public BrokerConnection removeConnection(UUID nodeId) {
        return brokerConnections.remove(nodeId);
    }

    public static BrokerConnections getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new BrokerConnections();
        }

        return INSTANCE;
    }

    private boolean containsMetadataManagerConnection() {
        Optional<BrokerConnection> brokerConnection = brokerConnections.values().stream()
                .filter(connection -> connection.connectionType().equals(ConnectionType.METADATA_MANAGER))
                .findFirst();

        if (brokerConnection.isPresent()) {
            Socket socket = brokerConnection.get().socket();
            SocketCommunicator.sendMessage(socket, "Metadata manager with ip %s is already connected!".formatted(socket.getInetAddress().getHostAddress()));
            return true;
        }

        return false;
    }

}
