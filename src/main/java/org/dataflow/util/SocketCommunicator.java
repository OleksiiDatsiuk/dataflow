package org.dataflow.util;

import lombok.NoArgsConstructor;
import org.dataflow.exception.InternalServerError;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.stream.Collectors;

@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class SocketCommunicator {

    public static void sendMessage(Socket clientSocket, String message) {
        try (PrintWriter streamWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {
            streamWriter.println(message);
        } catch (IOException e) {
            throw new InternalServerError("Failed to send message to a client socket", e);
        }
    }

    public static String receiveMessage(Socket socket) {
        try {
            BufferedInputStream bufferedInputStream = new BufferedInputStream(socket.getInputStream());

            byte[] buffer = new byte[1024 * 10]; // 10KB

            int actualRead = bufferedInputStream.read(buffer);
            if (actualRead == -1) {
                throw new IOException("End of stream reached");
            }

            InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
            String encoding = inputStreamReader.getEncoding();

            if (encoding == null) {
                encoding = "UTF-8";
            }
            return new String(buffer, 0, actualRead, Charset.forName(encoding));
        } catch (IOException e) {
            Sockets.close(socket);
            throw new InternalServerError("Failed to retrieve data from the socket!", e);
        }
    }

}
