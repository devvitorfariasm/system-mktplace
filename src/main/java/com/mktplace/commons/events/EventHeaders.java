package com.mktplace.commons.events;

/**
 * Headers Kafka de todo evento de negócio. Metadados viajam aqui, nunca no payload.
 */
public final class EventHeaders {

    public static final String EVENT_ID = "eventId";
    public static final String EVENT_TYPE = "eventType";
    public static final String SCHEMA_VERSION = "schemaVersion";
    public static final String CORRELATION_ID = "correlationId";
    public static final String CAUSATION_ID = "causationId";
    public static final String PRODUCER = "producer";
    public static final String PRODUCED_AT = "producedAt";
    public static final String ORDER_ID = "orderId";
    /** W3C Trace Context; preenchido pelo Micrometer Tracing quando a observação do template está ligada. */
    public static final String TRACEPARENT = "traceparent";
    /** Id da mensagem de DLQ que originou a republicação. */
    public static final String REPROCESSED_FROM = "x-reprocessed-from";
    public static final String REPROCESS_ATTEMPT = "x-reprocess-attempt";

    /** Gravados pelo error handler ao recuperar para a DLT: grupo que falhou e número da última tentativa. */
    public static final String DLQ_CONSUMER_GROUP = "x-dlq-consumer-group";
    public static final String DLQ_ATTEMPTS = "x-dlq-attempts";
    public static final String DLQ_PREFIX = "x-dlq-";

    /** Header HTTP que vira o correlationId da cadeia de eventos. */
    public static final String HTTP_CORRELATION_ID = "X-Correlation-Id";

    private EventHeaders() {
    }
}
