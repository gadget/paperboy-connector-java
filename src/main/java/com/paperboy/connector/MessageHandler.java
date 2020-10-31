package com.paperboy.connector;

@FunctionalInterface
public interface MessageHandler {

    void handleMessage(String topic, String message);

}
