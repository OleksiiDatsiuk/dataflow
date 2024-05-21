package org.dataflow.producer.client;

import lombok.extern.slf4j.Slf4j;
import org.dataflow.producer.common.RequestType;
import org.dataflow.producer.dto.NodeRequest;
import org.dataflow.producer.dto.ProducerMessage;
import org.dataflow.producer.util.SocketCommunicator;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.UUID;

@Slf4j
public class ProducerClient {

    private final Socket socket;
    private final PrintWriter writer;
    private final UUID id;

    public ProducerClient(String host, int port) {
        this.socket = establishConnectionToBroker(host, port);

        try {
            this.writer = new PrintWriter(socket.getOutputStream(), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        UUID id = UUID.randomUUID();
        this.id = id;

        NodeRequest initialRequest = NodeRequest.builder()
                .nodeId(id)
                .connectionType("PRODUCER")
                .requestType(RequestType.INITIAL_REQUEST)
                .build();
        SocketCommunicator.sendMessage(initialRequest.asJsonString(), writer);
    }

    private static Socket establishConnectionToBroker(String host, int port) {
        try {
            return new Socket(host, port);
        } catch (IOException e) {
            log.error("Failed to establish connection to the broker!", e);
            throw new RuntimeException("Failed to establish connection to the broker!", e);
        }
    }

    public void sendMessage(String topic, String message) {
        NodeRequest producerRequest = NodeRequest.builder()
                .nodeId(this.id)
                .connectionType("PRODUCER")
                .requestType(RequestType.BASIC_REQUEST)
                .message(new ProducerMessage(topic, message).asJsonString())
                .build();

        writer.println(producerRequest.asJsonString());
        if(writer.checkError()) {
            log.error("Failed to send message to the broker.");
        } else {
            log.info("Message sent successfully: {}", message);
        }
    }

    public void close() {
        try {
            socket.close();
            log.info("Connection to the broker closed successfully.");
        } catch (IOException e) {
            log.error("Failed to close the connection to the broker!", e);
        }
    }
}
