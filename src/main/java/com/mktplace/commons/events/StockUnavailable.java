package com.mktplace.commons.events;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/** Pelo menos um item sem estoque suficiente; nenhuma reserva é mantida. */
public record StockUnavailable(
        UUID orderId,
        List<Shortage> shortages,
        OffsetDateTime occurredAt) implements DomainEvent {

    public record Shortage(String sku, int requested, int available) {
    }

    @Override
    public String eventType() {
        return EventTypes.STOCK_UNAVAILABLE;
    }

    @Override
    public String key() {
        return orderId.toString();
    }
}
