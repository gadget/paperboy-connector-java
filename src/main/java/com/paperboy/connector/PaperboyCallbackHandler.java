package com.paperboy.connector;

public interface PaperboyCallbackHandler {

    void onUserConnected(MessageSender msgSender, String userId, String channel);

    boolean hasAccess(String userId, String channel);

}
