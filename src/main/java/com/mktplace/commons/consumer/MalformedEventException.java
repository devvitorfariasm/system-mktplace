package com.mktplace.commons.consumer;

/** Payload ou headers inválidos: JSON quebrado, header obrigatório ausente. */
public class MalformedEventException extends NonRetryableEventException {

    public MalformedEventException(String message) {
        super(message);
    }

    public MalformedEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
