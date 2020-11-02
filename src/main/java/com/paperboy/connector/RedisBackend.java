package com.paperboy.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

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
    public void listen(String queue, MessageHandler messageHandler) {
        Jedis jedis = jedisPool.getResource();
        while (true) {
            List<String> messages = jedis.blpop(10, queue);
            if (messages != null) {
                // Redis always returns the name of the list as the 1st element so we skip
                messages.stream().skip(1).forEach(m -> {
                    messageHandler.handleMessage(queue, m);
                });
            }
        }
    }
}
