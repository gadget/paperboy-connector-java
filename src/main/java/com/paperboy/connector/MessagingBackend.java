package com.paperboy.connector;

public interface MessagingBackend {

    void init();

    void publish(String topic, Object msg);

    void subscribe(String topic, MessageHandler messageHandler);

}
