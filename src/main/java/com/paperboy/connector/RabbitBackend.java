package com.paperboy.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeoutException;

public class RabbitBackend implements MessagingBackend {

    private static final ThreadLocal<Channel> channelThreadLocal = new ThreadLocal<>();

    private final ConnectionFactory connectionFactory;
    private final ObjectMapper objectMapper;
    private Connection publisherConnection;

    public RabbitBackend(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        this.objectMapper = new ObjectMapper();
        // TODO: configure objectMapper
    }

    @Override
    public void init() {
        try {
            this.publisherConnection = connectionFactory.newConnection();
        } catch (IOException e) {
            throw new UncheckedIOException("Unexpected IO error!", e);
        } catch (TimeoutException e) {
            throw new RuntimeException("RabbitMQ timeout error!", e);
        }
    }

    @Override
    public void publish(String topic, Object msg) {
        try {
            String msgString = objectMapper.writeValueAsString(msg);
            Channel channel = channelThreadLocal.get();
            if (channel == null) {
                channel = publisherConnection.createChannel();
                channelThreadLocal.set(channel);
            }
            channel.basicPublish(topic, "", null, msgString.getBytes());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize message!", e);
        } catch (IOException e) {
            throw new UncheckedIOException("Unexpected IO error!", e);
        }
    }

    @Override
    public void listen(String queue, MessageHandler messageHandler) {
        try {
            Channel channel = connectionFactory.newConnection().createChannel();
            channel.basicConsume(queue, false, (consumerTag, message) -> {
                messageHandler.handleMessage(queue, new String(message.getBody()));
                channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
            }, consumerTag -> {
                // nop
            });
        } catch (IOException e) {
            throw new UncheckedIOException("Unexpected IO error!", e);
        } catch (TimeoutException e) {
            throw new RuntimeException("RabbitMQ timeout error!", e);
        }
    }

}
