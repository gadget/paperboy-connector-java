package com.paperboy.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class RabbitBackend implements MessagingBackend {

    private final ConnectionFactory connectionFactory;
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<Thread, Channel> publisherChannels = new ConcurrentHashMap<>();
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

    private Channel channelForThread() {
        publisherChannels.computeIfAbsent(Thread.currentThread(), k -> {
            try {
                return publisherConnection.createChannel();
            } catch (IOException e) {
                throw new UncheckedIOException("Unexpected IO error!", e);
            }
        });
        return publisherChannels.get(Thread.currentThread());
    }

    @Override
    public void publish(String topic, Object msg) {
        try {
            String msgString = objectMapper.writeValueAsString(msg);
            channelForThread().basicPublish("", topic, null, msgString.getBytes());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize message!", e);
        } catch (IOException e) {
            throw new UncheckedIOException("Unexpected IO error!", e);
        }
    }

    @Override
    public void subscribe(String topic, MessageHandler messageHandler) {
        try {
            connectionFactory.newConnection().createChannel().basicConsume(topic, (consumerTag, message) -> {
                messageHandler.handleMessage(topic, String.valueOf(message.getBody()));
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
