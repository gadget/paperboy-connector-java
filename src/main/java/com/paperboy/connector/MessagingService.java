package com.paperboy.connector;

import com.auth0.jwt.exceptions.JWTVerificationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessagingService implements MessageSender {

    private static final Log LOG = LogFactory.getLog(MessagingService.class);

    private final MessagingBackend messagingBackend;
    private final AuthorizationTokenService authorizationTokenService;
    private final PaperboyCallbackHandler paperboyCallbackHandler;
    private final ExecutorService executorService;
    private final ObjectMapper objectMapper;

    public MessagingService(MessagingBackend messagingBackend, AuthorizationTokenService authorizationTokenService, PaperboyCallbackHandler paperboyCallbackHandler) {
        this.messagingBackend = messagingBackend;
        this.authorizationTokenService = authorizationTokenService;
        this.paperboyCallbackHandler = paperboyCallbackHandler;
        this.executorService = Executors.newSingleThreadExecutor();
        this.objectMapper = new ObjectMapper();
        // TODO: configure objectMapper
    }

    public void sendToUser(String userId, Payload payload) {
        Message msg = new Message(userId, null, payload);
        messagingBackend.publish("paperboy-message", msg);
    }

    public void sendToChannel(String channel, Payload payload) {
        Message msg = new Message(null, channel, payload);
        messagingBackend.publish("paperboy-message", msg);
    }

    public void sendSubscriptionCloseMessage(String userId, String channel) {
        AuthorizationMessage msg = new AuthorizationMessage(null, null, userId, channel);
        messagingBackend.publish("paperboy-subscription-close", msg);
    }

    public void init() {
        messagingBackend.init();
        executorService.submit(() -> {
            try {
                LOG.info("Starting Paperboy subscription request listener...");
                messagingBackend.listen("paperboy-subscription-request", (channel, message) -> {
                    try {
                        AuthorizationMessage msgIn = objectMapper.readValue(message, AuthorizationMessage.class);
                        AuthorizationMessage msgOut = authorizationTokenService.authorize(msgIn.getToken(), msgIn.getWsId());
                        messagingBackend.publish("paperboy-subscription-authorized", msgOut);
                        LOG.info(String.format("Successful authorization for '%s'.", msgOut.getWsId()));
                        paperboyCallbackHandler.onSubscription(MessagingService.this, msgOut.getUserId(), msgOut.getChannel());
                    } catch (JsonProcessingException e) {
                        LOG.error("Could not deserialize subscription request!", e);
                    } catch (JWTVerificationException e) {
                        LOG.error("Error during token verification!", e);
                    } catch (Exception e) {
                        LOG.error("Unexpected error during authorization!", e);
                    }
                });
            } catch (Exception e) {
                LOG.error("Unexpected error during topic subscription!", e);
            }
        });
    }

}
