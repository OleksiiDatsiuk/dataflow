package org.dataflow.model;

import org.dataflow.common.ConnectionType;
import org.dataflow.common.RequestType;

import java.util.UUID;

public record NodeRequest(UUID nodeId, ConnectionType connectionType, RequestType requestType, String message) {
}
