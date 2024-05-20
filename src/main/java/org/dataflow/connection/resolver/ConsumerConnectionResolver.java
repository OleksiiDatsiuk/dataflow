package org.dataflow.connection.resolver;

import org.dataflow.broker.ManagedBroker;
import org.dataflow.data.Partition;
import org.dataflow.data.Topic;
import org.dataflow.data.serializer.Serializer;
import org.dataflow.dto.ConsumerMessage;
import org.dataflow.dto.ToConsumeMessage;
import org.dataflow.model.BrokerConnection;
import org.dataflow.model.NodeRequest;
import org.dataflow.util.SocketCommunicator;

import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerConnectionResolver implements Resolvable {

    private final ManagedBroker managedBroker = ManagedBroker.getInstance();

    @Override
    public void resolve(BrokerConnection brokerConnection, NodeRequest nodeRequest) {
        switch (nodeRequest.requestType()) {
            case BASIC_REQUEST:
                handleBasicRequest(brokerConnection, nodeRequest);
                break;
            case ACK_REQUEST:
                handleAcknowledgeRequest(nodeRequest);
                break;
            case COMMIT_OFFSET_REQUEST:
                handleCommitOffsetRequest(brokerConnection, nodeRequest);
                break;
            default:
                throw new IllegalArgumentException("Unsupported request type: " + nodeRequest.requestType());
        }
    }

    private void handleBasicRequest(BrokerConnection brokerConnection, NodeRequest nodeRequest) {
        ConsumerMessage consumerMessage = Serializer.parseToConsumerMessage(nodeRequest.message());
        List<ToConsumeMessage> messages = fetchMessagesForConsumer(brokerConnection, consumerMessage, nodeRequest.nodeId());
        String response = Serializer.toJsonString(messages);
        SocketCommunicator.sendMessage(brokerConnection.socket(), response);
    }

    private List<ToConsumeMessage> fetchMessagesForConsumer(BrokerConnection brokerConnection, ConsumerMessage consumerMessage, UUID consumerId) {
        Topic topic = managedBroker.getTopic(consumerMessage.getTopic());

        if (topic == null) {
            throw new IllegalArgumentException("Topic with name " + consumerMessage.getTopic() + " does not exist");
        }

        long lastOffset = getLastOffsetForConsumer(brokerConnection.socket(), consumerMessage.getTopic());

        List<ToConsumeMessage> messages = new ArrayList<>();
        for (Partition partition : topic.getPartitions()) {
            int partitionId = topic.getPartitions().indexOf(partition);
            messages.addAll(topic.getMessagesForConsumer(consumerId, partitionId, 10));
        }

        updateLastOffsetForConsumer(brokerConnection.socket(), consumerMessage.getTopic(), lastOffset + messages.size());

        return messages;
    }

    private void handleAcknowledgeRequest(NodeRequest nodeRequest) {
        ConsumerMessage consumerMessage = Serializer.parseToConsumerMessage(nodeRequest.message());
        String acknowledgmentKey = generateAcknowledgmentKey(consumerMessage.getTopic(), consumerMessage.getPartition(), consumerMessage.getOffset());

        managedBroker.getAcknowledgments().computeIfAbsent(nodeRequest.nodeId(), k -> ConcurrentHashMap.newKeySet()).add(acknowledgmentKey);

        System.out.println("Acknowledgment received for consumer: " + nodeRequest.nodeId() + " for " + acknowledgmentKey);
    }

    private void handleCommitOffsetRequest(BrokerConnection brokerConnection, NodeRequest nodeRequest) {
        ConsumerMessage consumerMessage = Serializer.parseToConsumerMessage(nodeRequest.message());
        updateLastOffsetForConsumer(brokerConnection.socket(), consumerMessage.getTopic(), consumerMessage.getOffset());
        System.out.println("Offset committed for consumer: " + nodeRequest.nodeId() + " at offset: " + consumerMessage.getOffset());
    }

    private long getLastOffsetForConsumer(Socket consumerSocket, String topicName) {
        return managedBroker.getConsumerOffsets().getOrDefault(consumerSocket, new HashMap<>()).getOrDefault(topicName, 0L);
    }

    private void updateLastOffsetForConsumer(Socket consumerSocket, String topicName, long newOffset) {
        managedBroker.getConsumerOffsets().computeIfAbsent(consumerSocket, k -> new HashMap<>()).put(topicName, newOffset);
    }

    private String generateAcknowledgmentKey(String topic, int partition, long offset) {
        return topic + "-" + partition + "-" + offset;
    }

}
