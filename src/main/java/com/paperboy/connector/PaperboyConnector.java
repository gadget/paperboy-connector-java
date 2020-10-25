package com.paperboy.connector;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisPool;

import java.security.Principal;

public class PaperboyConnector {

    private final AuthorizationTokenService authorizationTokenService;
    private final MessagingService messagingService;

    public PaperboyConnector(JedisPool jedisPool, PaperboyCallbackHandler paperboyCallbackHandler) {
        this.authorizationTokenService = new AuthorizationTokenService(paperboyCallbackHandler);
        this.messagingService = new MessagingService(jedisPool, this.authorizationTokenService, paperboyCallbackHandler);
    }

    public void startListening() {
        messagingService.startListening();
    }

    public String requestToken(Principal principal, String channel) {
        if (principal == null || StringUtils.isBlank(principal.getName())) {
            throw new RuntimeException("Authentication required!");
        }

        return authorizationTokenService.generateToken(principal.getName(), channel);
    }

    public void sendToUser(String userId, Payload payload) {
        messagingService.sendToUser(userId, payload);
    }

    public void sendToChannel(String channel, Payload payload) {
        messagingService.sendToChannel(channel, payload);
    }

}
