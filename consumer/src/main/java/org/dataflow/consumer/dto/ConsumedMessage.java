package org.dataflow.consumer.dto;

import lombok.Data;

@Data
public class ConsumedMessage {

    private String message;
    private int partitionId;
    private int currentOffset;

}
