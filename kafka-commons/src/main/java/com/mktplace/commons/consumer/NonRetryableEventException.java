package com.mktplace.commons.consumer;

/** Base das falhas que não devem passar pelo retry: vão direto para a DLT. */
public abstract class NonRetryableEventException extends RuntimeException {

    protected NonRetryableEventException(String message) {
        super(message);
    }

    protected NonRetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
