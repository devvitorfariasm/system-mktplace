package com.mktplace.commons.events;

import org.jspecify.annotations.Nullable;

import java.time.OffsetDateTime;
import java.util.UUID;

/** Ajuste manual de estoque. Evento operacional: não há pedido, a chave é o sku. */
public record StockAdjusted(
        String sku,
        int delta,
        int balanceAfter,
        String reason,
        OffsetDateTime adjustedAt) implements DomainEvent {

    @Override
    public String eventType() {
        return EventTypes.STOCK_ADJUSTED;
    }

    @Override
    public String key() {
        return sku;
    }

    @Override
    public @Nullable UUID orderId() {
        return null;
    }
}
