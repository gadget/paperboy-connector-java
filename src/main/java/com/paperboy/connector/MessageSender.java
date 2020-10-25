package com.paperboy.connector;

public interface MessageSender {

    void sendToUser(String userId, Payload payload);

    void sendToChannel(String channel, Payload payload);

}
