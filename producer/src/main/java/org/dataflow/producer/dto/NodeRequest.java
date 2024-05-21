package org.dataflow.producer.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;
import org.dataflow.producer.common.RequestType;

import java.util.UUID;

@Data
@Builder
public class NodeRequest {

    private UUID nodeId;
    private String connectionType;
    private RequestType requestType;
    private String message;

    public String asJsonString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
