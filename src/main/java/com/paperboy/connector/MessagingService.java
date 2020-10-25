package com.paperboy.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessagingService implements MessageSender {

    private final JedisPool jedisPool;
    private final AuthorizationTokenService authorizationTokenService;
    private final PaperboyCallbackHandler paperboyCallbackHandler;
    private final ExecutorService executorService;
    private final ObjectMapper objectMapper;

    public MessagingService(JedisPool jedisPool, AuthorizationTokenService authorizationTokenService, PaperboyCallbackHandler paperboyCallbackHandler) {
        this.jedisPool = jedisPool;
        this.authorizationTokenService = authorizationTokenService;
        this.paperboyCallbackHandler = paperboyCallbackHandler;
        this.executorService = Executors.newSingleThreadExecutor();
        this.objectMapper = new ObjectMapper();
        // TODO: configure objectMapper
    }

    private void sendMessage(String channel, Object msg) {
        try {
            jedisPool.getResource().publish(channel, objectMapper.writeValueAsString(msg));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not serialize message!", e);
        }
    }

    public void sendToUser(String userId, Payload payload) {
        Message msg = new Message(userId, null, payload);
        sendMessage("paperboy-message", msg);
    }

    public void sendToChannel(String channel, Payload payload) {
        Message msg = new Message(null, channel, payload);
        sendMessage("paperboy-message", msg);
    }

    // subscribes to messaging backend (Redis) for connection requests
    public void startListening() {
        executorService.submit(() -> {
            jedisPool.getResource().subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    AuthorizationMessage msgIn;
                    try {
                        msgIn = objectMapper.readValue(message, AuthorizationMessage.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Could not deserialize connection request!", e);
                    }
                    AuthorizationMessage msgOut = authorizationTokenService.authorize(msgIn.getToken(), msgIn.getWsId());
                    sendMessage("paperboy-connection-authorized", msgOut);
                    paperboyCallbackHandler.onUserConnected(MessagingService.this, msgOut.getUserId(), msgOut.getChannel());
                }
            }, "paperboy-connection-request");
        });
    }

}
