package org.dataflow.consumer.client;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dataflow.consumer.common.RequestType;
import org.dataflow.consumer.dto.ConsumedMessage;
import org.dataflow.consumer.dto.ConsumerMessage;
import org.dataflow.consumer.dto.NodeRequest;
import org.dataflow.consumer.serializer.Serializer;
import org.dataflow.consumer.util.SocketCommunicator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.dataflow.consumer.common.RequestType.INITIAL_REQUEST;

@Slf4j
@Builder(access = AccessLevel.PRIVATE)
public class ConsumerClient {

    private static final String CONSUMER_CONNECTION_TYPE = "CONSUMER";

    @Getter
    private final UUID id;

    @Getter
    private final String bootstrapServer;

    @Getter
    private final int brokerPort;

    @Getter
    private final String topicName;

    @Getter
    private final int partition;

    @Getter
    private long offset;

    private Socket socket;
    private PrintWriter socketWriter;
    private BufferedReader socketReader;

    public static ConsumerClient createConsumer(String host, int port, String topic) {
        Socket brokerSocket = establishConnectionToBroker(host, port);

        PrintWriter brokerWriter;
        BufferedReader bufferedReader;
        try {
            brokerWriter = new PrintWriter(brokerSocket.getOutputStream(), true);
            bufferedReader = new BufferedReader(new InputStreamReader(brokerSocket.getInputStream()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ConsumerClient consumerClient = ConsumerClient.builder()
                .id(UUID.randomUUID())
                .bootstrapServer(host)
                .brokerPort(port)
                .topicName(topic)
                .partition(0)
                .offset(0)
                .socket(brokerSocket)
                .socketWriter(brokerWriter)
                .socketReader(bufferedReader)
                .build();

        NodeRequest initialRequest = NodeRequest.builder()
                .nodeId(consumerClient.id)
                .connectionType(CONSUMER_CONNECTION_TYPE)
                .requestType(INITIAL_REQUEST)
                .build();

        consumerClient.socketWriter.println(initialRequest.asJsonString());
        return consumerClient;
    }

    public void consume(Consumer<String> consumer) {
        while (true) {
            NodeRequest nodeRequest = NodeRequest.builder()
                    .nodeId(this.id)
                    .connectionType(CONSUMER_CONNECTION_TYPE)
                    .requestType(RequestType.BASIC_REQUEST)
                    .message(new ConsumerMessage(topicName, partition, offset).asJsonString())
                    .build();

            SocketCommunicator.sendMessage(socketWriter, nodeRequest.asJsonString());
            String unparsedMessages = socketReader.lines().collect(Collectors.joining());

            if (unparsedMessages.isBlank()) {
                continue;
            }

            List<ConsumedMessage> messages = Serializer.parseToConsumedMessages(unparsedMessages);

            processMessages(consumer, messages);
            acknowledgeMessages();
            commitOffset(messages.size() + offset);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void processMessages(Consumer<String> consumer, List<ConsumedMessage> messages) {
        System.out.println("Received messages: " + messages);
        messages.stream()
                .map(ConsumedMessage::getMessage)
                .forEach(consumer);
        this.offset += messages.size();
    }

    private void acknowledgeMessages() {
        NodeRequest ackRequest = NodeRequest.builder()
                .nodeId(this.id)
                .connectionType(CONSUMER_CONNECTION_TYPE)
                .requestType(RequestType.ACK_REQUEST)
                .message(new ConsumerMessage(topicName, partition, offset).asJsonString())
                .build();

        SocketCommunicator.sendMessage(socketWriter, ackRequest.asJsonString());
    }

    private void commitOffset(long newOffset) {
        NodeRequest commitRequest = NodeRequest.builder()
                .nodeId(this.id)
                .connectionType(CONSUMER_CONNECTION_TYPE)
                .requestType(RequestType.COMMIT_OFFSET_REQUEST)
                .message(new ConsumerMessage(topicName, partition, newOffset).asJsonString())
                .build();

        SocketCommunicator.sendMessage(socketWriter, commitRequest.asJsonString());
    }

    public void disconnect() {
        if (socket == null) {
            return;
        }

        if (!socket.isConnected()) {
            return;
        }

        try {
            socket.close();
        } catch (IOException e) {
            log.error("Connection couldn't be closed!");
            throw new RuntimeException(e);
        }
    }

    private static Socket establishConnectionToBroker(String host, int port) {
        try {
            return new Socket(host, port);
        } catch (IOException e) {
            log.error("Failed to establish connection to the broker!", e);
            throw new RuntimeException("Failed to establish connection to the broker!", e);
        }
    }

}
