package com.mktplace.commons.consumer;

/** Mensagem "venenosa": reprocessar nunca vai funcionar (ex.: chaos POISON). */
public class PoisonMessageException extends NonRetryableEventException {

    public PoisonMessageException(String message) {
        super(message);
    }
}
