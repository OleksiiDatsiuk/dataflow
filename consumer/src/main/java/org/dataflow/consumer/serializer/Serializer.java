package org.dataflow.consumer.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dataflow.consumer.dto.ConsumedMessage;

import java.util.ArrayList;
import java.util.List;

public class Serializer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static List<ConsumedMessage> parseToConsumedMessages(String message) {
        try {
            return OBJECT_MAPPER.readValue(message, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            return new ArrayList<>();
        }
    }

}
