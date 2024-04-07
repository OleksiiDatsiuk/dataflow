package org.dataflow.server;

import lombok.extern.slf4j.Slf4j;
import org.dataflow.util.SocketCommunicator;
import org.dataflow.exception.InternalServerError;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractServer {

    protected final int port;
    protected ServerSocket socket;
    protected final ExecutorService executorService = Executors.newCachedThreadPool();

    public AbstractServer(int port) {
        this.port = port;
    }

    public abstract void run();

    public void shutDown() {
        try {
            log.info("Closing server {}:{}", socket.getInetAddress().getHostAddress(), socket.getLocalPort());
            socket.close();
        } catch (IOException e) {
            throw new InternalServerError("Failed to close server!", e);
        }
    }

    protected void processRequests(BiConsumer<String, Socket> subscriber) {
        log.info("Server is Waiting for requests!");

        while (!socket.isClosed()) {
            try {
                Socket clientSocket = socket.accept();
                executorService.submit(() -> handleClient(clientSocket, subscriber));
            } catch (SocketException e) {
                log.info("Connection interrupted! Server is closed!");
            } catch (IOException e) {
                log.info("Connection to the client socket couldn't be established!");
            }
        }

        executorService.shutdown();
    }

    protected void start() {
        try {
            if (Objects.isNull(socket)) {
                socket = new ServerSocket(port);
            }
        } catch (IOException ex) {
            throw new InternalServerError("Failed to start server!", ex);
        }
    }

    private void handleClient(Socket clientSocket, BiConsumer<String, Socket> subscriber) {
        log.info("Successfully established connection to {}", clientSocket.getInetAddress());

        while (!clientSocket.isClosed()) {
            BufferedReader clientInput = SocketCommunicator.receiveMessage(clientSocket);
            executorService.execute(() -> subscriber.accept(clientInput.lines().collect(Collectors.joining()), clientSocket));
        }
    }

}
