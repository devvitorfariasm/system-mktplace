package com.mktplace.commons.audit;

/** O que o registro de auditoria descreve. */
public enum AuditKind {
    /** Um produtor publicou o evento (partição/offset confirmados pelo broker). */
    PRODUCED,
    /** Um consumer group fez uma tentativa de processar o evento. */
    CONSUMED,
    /** Um operador descartou definitivamente a mensagem da DLQ. */
    DLQ_DISCARDED
}
