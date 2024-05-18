package org.dataflow.producer.client;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

@Slf4j
public class ProducerClient {

    private final Socket socket;
    private final PrintWriter writer;

    private ProducerClient(Socket socket) throws IOException {
        this.socket = socket;
        this.writer = new PrintWriter(socket.getOutputStream(), true);
    }

    public static ProducerClient createProducer(String host, int port) {
        try {
            Socket brokerSocket = establishConnectionToBroker(host, port);
            return new ProducerClient(brokerSocket);
        } catch (IOException e) {
            log.error("Failed to create ProducerClient!", e);
            throw new RuntimeException("Failed to initialize the ProducerClient", e);
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

    public void sendMessage(String message) {
        writer.println(message);
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
