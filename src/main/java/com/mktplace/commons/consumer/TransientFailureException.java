package com.mktplace.commons.consumer;

/** Falha temporária (ex.: chaos TRANSIENT, timeout de gateway): passa pelo retry com backoff. */
public class TransientFailureException extends RuntimeException {

    public TransientFailureException(String message) {
        super(message);
    }
}
