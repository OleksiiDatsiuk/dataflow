package org.dataflow.connection.resolver;

import org.dataflow.broker.ManagedBroker;
import org.dataflow.data.Partition;
import org.dataflow.data.Topic;
import org.dataflow.model.BrokerConnection;
import org.dataflow.model.NodeRequest;
import org.dataflow.util.SocketCommunicator;

import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ConsumerConnectionResolver implements Resolvable {

    private final ManagedBroker managedBroker = ManagedBroker.getInstance();
    private final Map<Socket, Map<String, Integer>> consumerOffsets = new HashMap<>();

    @Override
    public void resolve(BrokerConnection brokerConnection, NodeRequest nodeRequest) {
        handleBasicRequest(brokerConnection, nodeRequest);
    }

    private void handleBasicRequest(BrokerConnection brokerConnection, NodeRequest nodeRequest) {
        List<String> messages = fetchMessagesForConsumer(brokerConnection, nodeRequest.nodeId(), nodeRequest.message());
        SocketCommunicator.sendMessage(brokerConnection.socket(), messages.toString());
    }

    private List<String> fetchMessagesForConsumer(BrokerConnection brokerConnection, UUID consumerId, String topicName) {
        Topic topic = managedBroker.getTopic(topicName);

        if (topic == null) {
            throw new IllegalArgumentException("Topic with name " + topicName + " does not exist");
        }

        int lastOffset = getLastOffsetForConsumer(brokerConnection.socket(), topicName);

        List<String> messages = new ArrayList<>();
        for (Partition partition : topic.getPartitions()) {
            int partitionId = topic.getPartitions().indexOf(partition);
            messages.addAll(topic.getMessagesForConsumer(consumerId, partitionId, 10));
        }

        updateLastOffsetForConsumer(brokerConnection.socket(), topicName, lastOffset + messages.size());

        return messages;
    }

    private int getLastOffsetForConsumer(Socket consumerSocket, String topicName) {
        return consumerOffsets.getOrDefault(consumerSocket, new HashMap<>()).getOrDefault(topicName, 0);
    }

    private void updateLastOffsetForConsumer(Socket consumerSocket, String topicName, int newOffset) {
        consumerOffsets.computeIfAbsent(consumerSocket, k -> new HashMap<>()).put(topicName, newOffset);
    }

}

