package org.dataflow.util;

import lombok.NoArgsConstructor;
import org.dataflow.exception.InternalServerError;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class SocketCommunicator {

    public static void sendMessage(Socket clientSocket, String message) {
        try (PrintWriter streamWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {
            streamWriter.println(message);
        } catch (IOException e) {
            throw new InternalServerError("Failed to send message to a client socket", e);
        }
    }

    public static BufferedReader receiveMessage(Socket socket) {
        try {
            return new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            Sockets.close(socket);
            throw new InternalServerError("Failed to retrieve data from the socket!", e);
        }
    }

}
