package org.dataflow.producer.util;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dataflow.producer.exception.InternalServerError;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

@Slf4j
@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class SocketCommunicator {

    public static void sendMessage(String message, PrintWriter writer) {
        writer.println(message);
        writer.flush();
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
