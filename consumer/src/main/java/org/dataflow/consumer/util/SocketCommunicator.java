package org.dataflow.consumer.util;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

@Slf4j
@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class SocketCommunicator {

    public static void sendMessage(Socket clientSocket, String message) {
        try (PrintWriter streamWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {
            streamWriter.println(message);
        } catch (IOException e) {
            throw new RuntimeException("Failed to send message to a client socket", e);
        }
    }

    public static BufferedReader receiveMessage(Socket socket) {
        try {
            return new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            try {
                socket.close();
            } catch (IOException ex) {
                throw new RuntimeException("Connection to the broker couldn't be closed!");
            }
            throw new RuntimeException("Failed to retrieve data from the socket!", e);
        }
    }

}