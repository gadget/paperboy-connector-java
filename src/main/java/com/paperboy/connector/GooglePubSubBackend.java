package com.paperboy.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;

public class GooglePubSubBackend implements MessagingBackend {

    private static final String SUBSCRIPTION_SUFFIX = "-sub-connector";
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<String, Publisher> publishers = new ConcurrentHashMap<>();

    public GooglePubSubBackend(JedisPool jedisPool) {
        this.objectMapper = new ObjectMapper();
        // TODO: configure objectMapper
    }

    @Override
    public void init() {

    }

    private Publisher publisherForTopic(String topic) {
        publishers.computeIfAbsent(topic, k -> {
            try {
                return Publisher.newBuilder(topic).build();
            } catch (IOException e) {
                throw new UncheckedIOException("Unexpected IO error!", e);
            }
        });
        return publishers.get(topic);
    }

    @Override
    public void publish(String topic, Object msg) {
        try {
            String msgString = objectMapper.writeValueAsString(msg);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(msgString)).build();
            publisherForTopic(topic).publish(pubsubMessage);
        } catch (IOException e) {
            throw new UncheckedIOException("Unexpected IO error!", e);
        }
    }

    @Override
    public void listen(String queue, MessageHandler messageHandler) {
        Subscriber subscriber = Subscriber.newBuilder(queue + SUBSCRIPTION_SUFFIX, (PubsubMessage message, AckReplyConsumer consumer) -> {
            messageHandler.handleMessage(queue, message.getData().toStringUtf8());
            consumer.ack();
        }).build();
        subscriber.startAsync().awaitRunning();
    }

}
