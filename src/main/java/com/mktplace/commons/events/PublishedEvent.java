package com.mktplace.commons.events;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.Nullable;

/** Resultado de uma publicação confirmada pelo broker (acks=all). */
public record PublishedEvent(
        String eventId,
        String eventType,
        String topic,
        int partition,
        long offset,
        String correlationId,
        @Nullable String causationId) {

    public static PublishedEvent from(ProducerRecord<String, String> record, RecordMetadata metadata) {
        var headers = record.headers();
        return new PublishedEvent(
                HeaderCodec.require(headers, EventHeaders.EVENT_ID),
                HeaderCodec.require(headers, EventHeaders.EVENT_TYPE),
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                HeaderCodec.require(headers, EventHeaders.CORRELATION_ID),
                HeaderCodec.string(headers, EventHeaders.CAUSATION_ID));
    }
}
