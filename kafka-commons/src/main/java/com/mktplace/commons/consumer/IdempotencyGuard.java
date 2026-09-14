package com.mktplace.commons.consumer;

import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.HeaderCodec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Idempotência do consumer: o INSERT em processed_events e o efeito de negócio acontecem na
 * mesma transação. Segunda entrega do mesmo eventId (retry após commit, reprocesso de DLQ,
 * outbox republicando) é ignorada sem efeito duplicado.
 */
@Component
public class IdempotencyGuard {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyGuard.class);

    private final ProcessedEventStore store;

    public IdempotencyGuard(ProcessedEventStore store) {
        this.store = store;
    }

    /** @return true se o trabalho foi executado; false se o evento já havia sido processado por este grupo. */
    @Transactional
    public boolean runOnce(ConsumerRecord<?, ?> record, String consumerGroup, Runnable work) {
        UUID eventId = eventId(record);
        if (!store.markProcessed(eventId, consumerGroup, record.topic())) {
            log.info("kafka.duplicate_ignored eventId={} group={}", eventId, consumerGroup);
            return false;
        }
        work.run();
        return true;
    }

    private static UUID eventId(ConsumerRecord<?, ?> record) {
        String raw = HeaderCodec.require(record.headers(), EventHeaders.EVENT_ID);
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new MalformedEventException("Header eventId não é um UUID: " + raw);
        }
    }
}
