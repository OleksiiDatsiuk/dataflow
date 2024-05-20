package org.dataflow.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ConsumerMessage {

    private String topic;
    private int partition;
    private long offset;

}
