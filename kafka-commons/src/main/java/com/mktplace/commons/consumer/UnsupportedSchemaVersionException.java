package com.mktplace.commons.consumer;

/** Versão de schema maior que a conhecida por este consumidor: melhor parar na DLT do que interpretar errado. */
public class UnsupportedSchemaVersionException extends NonRetryableEventException {

    public UnsupportedSchemaVersionException(String eventType, int received, int supported) {
        super("Evento %s v%d recebido; este consumidor entende até v%d".formatted(eventType, received, supported));
    }
}
