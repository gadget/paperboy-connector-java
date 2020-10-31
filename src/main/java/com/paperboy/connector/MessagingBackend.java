package com.paperboy.connector;

/**
 * Interface to implement by a supported messaging backend (e.g. Redis, RabbitMQ, etc.).
 */
public interface MessagingBackend {

    /**
     * Initialize resources.
     */
    void init();

    /**
     * Publish a message on the given topic/channel.
     *
     * @param topic name of the target topic
     * @param msg POJO of the message
     */
    void publish(String topic, Object msg);

    /**
     * Subscribes to a given topic/channel and registers a callback.
     *
     * @param topic name of the topic to subscribe
     * @param messageHandler callback
     */
    void subscribe(String topic, MessageHandler messageHandler);

}
