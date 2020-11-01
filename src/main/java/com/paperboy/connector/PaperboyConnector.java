package com.paperboy.connector;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.security.Principal;

public class PaperboyConnector {

    private static final Log LOG = LogFactory.getLog(PaperboyConnector.class);

    private final AuthorizationTokenService authorizationTokenService;
    private final MessagingService messagingService;

    public PaperboyConnector(MessagingBackend messagingBackend, PaperboyCallbackHandler paperboyCallbackHandler) {
        this.authorizationTokenService = new AuthorizationTokenService(paperboyCallbackHandler);
        this.messagingService = new MessagingService(messagingBackend, this.authorizationTokenService, paperboyCallbackHandler);
    }

    public void init() {
        messagingService.init();
        LOG.info("Paperboy listener started.");
    }

    public String generateToken(Principal principal, String channel) {
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

    public void closeSubscription(String userId, String channel) {
        messagingService.sendSubscriptionCloseMessage(userId, channel);
    }

}
