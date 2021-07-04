package com.paperboy.connector;

public class EmbeddedInstanceRemoteException extends Exception {

    public EmbeddedInstanceRemoteException(Exception e) {
        super(e);
    }

    public EmbeddedInstanceRemoteException(String msg) {
        super(msg);
    }

    public EmbeddedInstanceRemoteException(String msg, Exception e) {
        super(msg, e);
    }

}
