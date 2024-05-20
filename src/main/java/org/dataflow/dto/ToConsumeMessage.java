package org.dataflow.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ToConsumeMessage {

    private String message;
    private int partitionId;
    private int currentOffset;

}
