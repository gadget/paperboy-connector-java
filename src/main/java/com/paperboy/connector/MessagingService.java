package com.paperboy.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessagingService implements MessageSender {

    private static final Log LOG = LogFactory.getLog(MessagingService.class);

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
            throw new IllegalArgumentException("Could not serialize message!", e);
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

    public void startListening() {
        executorService.submit(() -> {
            jedisPool.getResource().subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    AuthorizationMessage msgIn;
                    AuthorizationMessage msgOut;
                    try {
                        msgIn = objectMapper.readValue(message, AuthorizationMessage.class);
                        msgOut = authorizationTokenService.authorize(msgIn.getToken(), msgIn.getWsId());
                    } catch (JsonProcessingException e) {
                        LOG.error("Could not deserialize authorization message!", e);
                        throw new IllegalArgumentException("Could not deserialize authorization message!", e);
                    } catch (Exception e) {
                        LOG.error("Error during authorization!", e);
                        throw e;
                    }

                    sendMessage("paperboy-connection-authorized", msgOut);
                    LOG.info(String.format("Successful authorization for '%s'.", msgOut.getWsId()));
                    paperboyCallbackHandler.onUserConnected(MessagingService.this, msgOut.getUserId(), msgOut.getChannel());
                }
            }, "paperboy-connection-request");
        });
    }

}