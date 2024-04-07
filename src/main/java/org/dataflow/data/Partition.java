package org.dataflow.data;

import java.util.ArrayList;
import java.util.List;

public class Partition {

    private List<String> messages;

    public Partition() {
        this.messages = new ArrayList<>();
    }

    public void addMessage(String message) {
        messages.add(message);

    }

    public List<String> getMessagesFromOffset(int offset, int count) {
        int toIndex = Math.min(offset + count, messages.size());
        if (offset > toIndex) {
            return new ArrayList<>();
        }
        return new ArrayList<>(messages.subList(offset, toIndex));
    }


}
