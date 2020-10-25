package com.paperboy.connector;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.util.Date;

public class AuthorizationTokenService {

    private static final String JWT_SECRET_ENV_KEY = "PAPERBOY_JWT_SECRET";
    private static final String JWT_ISSUER = "paperboy";

    private final PaperboyCallbackHandler paperboyCallbackHandler;
    private final Algorithm algorithm;
    private final JWTVerifier jwtVerifier;

    public AuthorizationTokenService(PaperboyCallbackHandler paperboyCallbackHandler) {
        String secret = System.getenv(JWT_SECRET_ENV_KEY);
        if (StringUtils.isBlank(secret)) {
            throw new IllegalArgumentException(String.format("Missing environment variable '%s'!", JWT_SECRET_ENV_KEY));
        }

        this.paperboyCallbackHandler = paperboyCallbackHandler;
        this.algorithm = Algorithm.HMAC256(secret);
        this.jwtVerifier = JWT.require(algorithm)
                .withIssuer(JWT_ISSUER)
                .build();
    }

    public String generateToken(String userId, String channel) {
        if (!paperboyCallbackHandler.hasAccess(userId, channel)) {
            throw new IllegalStateException(String.format("Access to channel '%s' for user '%s' is denied!", channel, userId));
        }

        return createToken(userId, channel);
    }

    private String createToken(String userId, String channel) {
        String token = JWT.create()
                .withIssuer(JWT_ISSUER)
                .withClaim("userId", userId)
                .withClaim("channel", channel)
                .withExpiresAt(DateUtils.addHours(new Date(), 8))
                .sign(algorithm);
        return token;
    }

    AuthorizationMessage authorize(String token, String wsId) {
        DecodedJWT jwt = jwtVerifier.verify(token);
        String userId = jwt.getClaim("userId").asString();
        String channel = jwt.getClaim("channel").asString();
        if (!paperboyCallbackHandler.hasAccess(userId, channel)) {
            throw new IllegalStateException(String.format("Access to channel '%s' for user '%s' denied!", channel, userId));
        }

        return new AuthorizationMessage(wsId, null, userId, channel);
    }

}
