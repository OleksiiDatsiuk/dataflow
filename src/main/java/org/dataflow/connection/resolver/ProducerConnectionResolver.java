package org.dataflow.connection.resolver;

import lombok.RequiredArgsConstructor;
import org.dataflow.broker.ManagedBroker;
import org.dataflow.data.Topic;
import org.dataflow.data.serializer.Serializer;
import org.dataflow.model.BrokerConnection;
import org.dataflow.model.NodeRequest;
import org.dataflow.model.ProducerMessage;

@RequiredArgsConstructor
public class ProducerConnectionResolver implements Resolvable {

    private final Serializer serializer;

    @Override
    public void resolve(BrokerConnection brokerConnection, NodeRequest nodeRequest) {
        ManagedBroker broker = ManagedBroker.getInstance();
        ProducerMessage producerMessage = serializer.parseToProducerMessage(nodeRequest.message());
        Topic topic = broker.getOrCreateTopic(producerMessage.topic());
        topic.addMessage(producerMessage.message());
    }

}
