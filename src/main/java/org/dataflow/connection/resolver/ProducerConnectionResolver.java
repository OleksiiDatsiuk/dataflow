package org.dataflow.connection.resolver;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dataflow.broker.ManagedBroker;
import org.dataflow.data.Topic;
import org.dataflow.data.serializer.Serializer;
import org.dataflow.model.BrokerConnection;
import org.dataflow.model.NodeRequest;
import org.dataflow.model.ProducerMessage;

@Slf4j
@RequiredArgsConstructor
public class ProducerConnectionResolver implements Resolvable {

    private final Serializer serializer;

    @Override
    public void resolve(BrokerConnection brokerConnection, NodeRequest nodeRequest) {
        log.info("Resolving producer connection for ip: {} and port: {}", brokerConnection.socket().getInetAddress(), brokerConnection.socket().getPort());

        ManagedBroker broker = ManagedBroker.getInstance();
        ProducerMessage producerMessage = serializer.parseToProducerMessage(nodeRequest.message());
        Topic topic = broker.getOrCreateTopic(producerMessage.topic());
        log.info("Producing message {} to topic {}", producerMessage.message(), producerMessage.topic());
        topic.addMessage(producerMessage.message());
    }

}
