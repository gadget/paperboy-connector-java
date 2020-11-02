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
     * @param msg   POJO of the message
     */
    void publish(String topic, Object msg);

    /**
     * Starts listening to a given queue and registers a callback.
     *
     * @param queue          name of the queue
     * @param messageHandler callback
     */
    void listen(String queue, MessageHandler messageHandler);

}
