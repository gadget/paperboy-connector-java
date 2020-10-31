package com.paperboy.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

public class RedisBackend implements MessagingBackend {

    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper;

    public RedisBackend(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        this.objectMapper = new ObjectMapper();
        // TODO: configure objectMapper
    }

    @Override
    public void init() {
        // nop
    }

    @Override
    public void publish(String topic, Object msg) {
        try {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.publish(topic, objectMapper.writeValueAsString(msg));
            }
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize message!", e);
        }
    }

    @Override
    public void subscribe(String topic, MessageHandler messageHandler) {
        jedisPool.getResource().subscribe(new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                messageHandler.handleMessage(channel, message);
            }
        }, topic);
    }
}
