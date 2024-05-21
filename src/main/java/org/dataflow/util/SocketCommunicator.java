package org.dataflow.util;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dataflow.exception.InternalServerError;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class SocketCommunicator {

    public static void sendMessage(Socket clientSocket, String message) {
        try (PrintWriter streamWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {
            streamWriter.println(message);
            streamWriter.flush();
        } catch (IOException e) {
            throw new InternalServerError("Failed to send message to a client socket", e);
        }
    }

    public static String receiveMessage(BufferedReader bufferedReader) {
        StringBuilder messageBuilder = new StringBuilder();
        try {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                messageBuilder.append(line);
                if (line.endsWith("}")) {
                    break;
                }
            }

            if (messageBuilder.length() == 0) {
                throw new IOException("End of stream reached or no data available");
            }

        } catch (IOException ignored) {

        }
        return messageBuilder.toString().trim();
    }


}
