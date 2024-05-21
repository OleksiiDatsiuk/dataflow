package org.dataflow.connection;

import org.dataflow.common.RequestType;
import org.dataflow.connection.resolver.ConnectionResolverFactory;
import org.dataflow.data.serializer.Serializer;
import org.dataflow.exception.InternalServerError;
import org.dataflow.model.NodeRequest;

import java.net.Socket;

public class ConnectionManager {

    private final BrokerConnections brokerConnections = BrokerConnections.getInstance();

    public void processConnection(String message, Socket connectedSocket) {
        NodeRequest nodeRequest = Serializer.parseToNodeRequest(message)
                .orElseThrow(() -> new InternalServerError("Invalid message received!"));

        if (nodeRequest.requestType() == RequestType.INITIAL_REQUEST) {
            brokerConnections.registerConnection(nodeRequest, connectedSocket);
            return;
        }

        ConnectionResolverFactory.getResolver(nodeRequest.connectionType())
                .resolve(brokerConnections.getConnection(nodeRequest.nodeId()), nodeRequest);
    }

}
