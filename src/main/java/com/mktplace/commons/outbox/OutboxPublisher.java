package com.mktplace.commons.outbox;

import com.mktplace.commons.events.DomainEvent;
import com.mktplace.commons.events.EventCodec;
import com.mktplace.commons.events.EventContext;
import com.mktplace.commons.events.EventContextHolder;
import com.mktplace.commons.util.UuidV7;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

/**
 * Publicação transacional: grava o evento na tabela outbox dentro da transação corrente.
 * O OutboxRelay envia ao Kafka depois do commit. Exige transação ativa: sem ela, o ponto do outbox se perde.
 */
@Component
public class OutboxPublisher {

    private final OutboxStore store;
    private final EventCodec codec;

    public OutboxPublisher(OutboxStore store, EventCodec codec) {
        this.store = store;
        this.codec = codec;
    }

    /** Usa o contexto da thread (correlationId da requisição HTTP ou do evento em processamento). */
    public UUID enqueue(DomainEvent event) {
        EventContext context = EventContextHolder.get();
        return enqueue(event, context.correlationIdOrNew(), context.causationId());
    }

    public UUID enqueue(DomainEvent event, String correlationId, @Nullable String causationId) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("OutboxPublisher.enqueue exige transação ativa (evento " + event.eventType() + ")");
        }
        UUID eventId = UuidV7.generate();
        store.insert(new OutboxStore.PendingEvent(eventId, event.eventType(), event.key(), event.orderId(),
                correlationId, causationId, event.schemaVersion(), codec.encode(event)));
        return eventId;
    }
}
