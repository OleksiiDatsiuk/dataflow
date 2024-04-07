package org.dataflow.model;

import lombok.Builder;
import org.dataflow.common.ConnectionType;

import java.net.Socket;

@Builder
public record BrokerConnection(ConnectionType connectionType, Socket socket) {
}
