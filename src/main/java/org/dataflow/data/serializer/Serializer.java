package org.dataflow.data.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dataflow.model.NodeRequest;
import org.dataflow.model.ProducerMessage;

import java.util.Optional;

public class Serializer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public Optional<NodeRequest> parseToNodeRequest(String message) {
        try {
            return Optional.of(OBJECT_MAPPER.readValue(message, NodeRequest.class));
        } catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }

    public ProducerMessage parseToProducerMessage(String message) {
        try {
            return OBJECT_MAPPER.readValue(message, ProducerMessage.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid message format");
        }
    }
}
