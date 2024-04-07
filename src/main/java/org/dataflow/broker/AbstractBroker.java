package org.dataflow.broker;

import lombok.Getter;
import org.dataflow.server.AbstractServer;
import org.dataflow.data.Topic;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public abstract class AbstractBroker extends AbstractServer {

    protected final UUID id;
    protected final String name;
    protected final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

    public AbstractBroker(int port, String name) {
        super(port);
        this.id = UUID.randomUUID();
        this.name = name;
    }

    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }

    public Topic getOrCreateTopic(String topicName) {
        Topic topic = this.topics.get(topicName);

        if (topic == null) {
            topic = Topic.builder()
                    .name(topicName)
                    .numberOfPartitions(5)
                    .build();
            this.topics.put(topicName, topic);
        }

        return topic;
    }

}
