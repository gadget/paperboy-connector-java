package com.paperboy.connector;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.JedisPool;

import java.security.Principal;

public class PaperboyConnector {

    private static final Log LOG = LogFactory.getLog(PaperboyConnector.class);

    private final AuthorizationTokenService authorizationTokenService;
    private final MessagingService messagingService;

    public PaperboyConnector(JedisPool jedisPool, PaperboyCallbackHandler paperboyCallbackHandler) {
        this.authorizationTokenService = new AuthorizationTokenService(paperboyCallbackHandler);
        this.messagingService = new MessagingService(jedisPool, this.authorizationTokenService, paperboyCallbackHandler);
    }

    public void startListening() {
        messagingService.startListening();
        LOG.info("Paperboy listener started.");
    }

    public String requestToken(Principal principal, String channel) {
        if (principal == null || StringUtils.isBlank(principal.getName())) {
            throw new IllegalArgumentException("Authentication required!");
        }

        LOG.debug(String.format("Generating token for '%s' to access channel '%s'.", principal.getName(), channel));
        return authorizationTokenService.generateToken(principal.getName(), channel);
    }

    public void sendToUser(String userId, Payload payload) {
        messagingService.sendToUser(userId, payload);
    }

    public void sendToChannel(String channel, Payload payload) {
        messagingService.sendToChannel(channel, payload);
    }

}
