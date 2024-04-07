package org.dataflow.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.Socket;

@Slf4j
public class Sockets {

    public static void close(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            log.info("Connection to the socket {} couldn't be closed!", socket.getInetAddress());
        }
    }

}
