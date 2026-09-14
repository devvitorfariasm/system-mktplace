package com.mktplace.commons.audit;

/** Resultado de uma tentativa de consumo. */
public enum ConsumeStatus {
    SUCCESS,
    /** Falhou e o container vai entregar de novo com backoff. */
    RETRY,
    /** Última tentativa ou exceção não-retryable: a mensagem foi para a DLT. */
    DLQ
}
