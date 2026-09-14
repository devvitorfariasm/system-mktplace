package com.mktplace.commons.events;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/** Pedido persistido com status CREATED. Valores em centavos. Schema v1. */
public record OrderCreated(
        UUID orderId,
        UUID customerId,
        List<Item> items,
        long total,
        String currency,
        OffsetDateTime createdAt) implements DomainEvent {

    public record Item(String sku, int quantity, long unitPrice) {
    }

    @Override
    public String eventType() {
        return EventTypes.ORDER_CREATED;
    }

    @Override
    public String key() {
        return orderId.toString();
    }
}
