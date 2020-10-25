package com.paperboy.connector;

import java.beans.ConstructorProperties;

public class AuthorizationMessage {

    private final String wsId;
    private final String token;
    private final String userId;
    private final String channel;

    @ConstructorProperties({"wsId", "token", "userId", "channel"})
    public AuthorizationMessage(String wsId, String token, String userId, String channel) {
        this.wsId = wsId;
        this.token = token;
        this.userId = userId;
        this.channel = channel;
    }

    public String getWsId() {
        return wsId;
    }

    public String getToken() {
        return token;
    }

    public String getUserId() {
        return userId;
    }

    public String getChannel() {
        return channel;
    }

}
