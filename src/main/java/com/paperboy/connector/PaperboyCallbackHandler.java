package com.paperboy.connector;

/**
 * Application callback interface.
 */
public interface PaperboyCallbackHandler {

    /**
     * Called when a client is successfully subscribed to a channel.
     *
     * @param msgSender Paperboy message sender instance for sending data to the client (like initial snapshots, etc.)
     * @param userId    user id of the client
     * @param channel   channel the client is subscribed to
     */
    void onSubscription(MessageSender msgSender, String userId, String channel);

    /**
     * Called when Paperboy is authorizing client subscriptions.
     *
     * @param userId  user id of the client
     * @param channel channel the client is subscribing to
     * @return true when the user is authorized to access channel, false otherwise
     */
    boolean hasAccess(String userId, String channel);

}
