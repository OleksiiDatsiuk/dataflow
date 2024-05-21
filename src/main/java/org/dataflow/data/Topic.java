package org.dataflow.data;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.dataflow.dto.ToConsumeMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Getter
@AllArgsConstructor
public class Topic {

    private String name;
    private List<Partition> partitions;
    private int numberOfPartitions;
    private Map<UUID, Map<Integer, Integer>> consumerOffsets;

    public Topic(String name, int numberOfPartitions) {
        this.name = name;
        this.partitions = new ArrayList<>();
        this.consumerOffsets = new HashMap<>();
        for (int i = 0; i < numberOfPartitions; i++) {
            this.partitions.add(new Partition());
        }
    }

    public void addMessage(String message) {
        int partitionIndex = selectPartitionIndexForMessage(message);
        partitions.get(partitionIndex).addMessage(message);
    }

    private int selectPartitionIndexForMessage(String message) {
        int nonNegativeHash = Math.abs(message.hashCode());
        return nonNegativeHash % partitions.size();
    }

    public void commitOffset(UUID consumerId, int partitionId, int offset) {
        consumerOffsets.computeIfAbsent(consumerId, k -> new HashMap<>()).put(partitionId, offset);
    }

    public int getOffset(UUID consumerId, int partitionId) {
        return consumerOffsets.getOrDefault(consumerId, new HashMap<>()).getOrDefault(partitionId, 0);
    }

    public List<ToConsumeMessage> getMessagesForConsumer(UUID consumerId, int partitionId, int maxMessages) {
        Partition partition = partitions.get(partitionId);
        int currentOffset = getOffset(consumerId, partitionId);
        List<String> rawMessages = partition.getMessagesFromOffset(currentOffset, maxMessages);
        List<ToConsumeMessage> messages = new ArrayList<>();
        for (String message : rawMessages) {
            messages.add(new ToConsumeMessage(message, partitionId, currentOffset));
            currentOffset++;
        }
        commitOffset(consumerId, partitionId, currentOffset);
        return messages;
    }

    public static TopicBuilder builder() {
        return new TopicBuilder();
    }

    public static class TopicBuilder {

        private String name;
        private int numberOfPartitions;

        public TopicBuilder name(String name) {
            this.name = name;
            return this;
        }

        public TopicBuilder numberOfPartitions(int numberOfPartitions) {
            this.numberOfPartitions = numberOfPartitions;
            return this;
        }

        public Topic build() {
            return new Topic(this.name, this.numberOfPartitions);
        }

    }
}
