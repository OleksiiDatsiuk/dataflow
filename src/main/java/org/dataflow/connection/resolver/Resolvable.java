package org.dataflow.connection.resolver;

import org.dataflow.model.BrokerConnection;
import org.dataflow.model.NodeRequest;

public interface Resolvable {

    void resolve(BrokerConnection brokerConnection, NodeRequest nodeRequest);

}
