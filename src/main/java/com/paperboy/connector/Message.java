package com.paperboy.connector;

public class Message {

    private final String userId;
    private final String channel;
    private final Payload payload;
    private final long timestamp;

    public Message(String userId, String channel, Payload payload) {
        this.userId = userId;
        this.channel = channel;
        this.payload = payload;
        this.timestamp = System.currentTimeMillis();
    }

    public String getUserId() {
        return userId;
    }

    public String getChannel() {
        return channel;
    }

    public Payload getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

}
