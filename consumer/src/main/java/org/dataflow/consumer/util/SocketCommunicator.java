package org.dataflow.consumer.util;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;

@Slf4j
@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class SocketCommunicator {

    public static void sendMessage(PrintWriter socketPrintWriter, String message) {
        socketPrintWriter.println(message);
        socketPrintWriter.flush();
    }

}