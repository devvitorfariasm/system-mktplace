package com.mktplace.commons.events;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/** Estoque reservado para todos os itens do pedido. */
public record StockReserved(
        UUID orderId,
        List<Reservation> reservations,
        OffsetDateTime reservedAt) implements DomainEvent {

    public record Reservation(UUID reservationId, String sku, int quantity) {
    }

    @Override
    public String eventType() {
        return EventTypes.STOCK_RESERVED;
    }

    @Override
    public String key() {
        return orderId.toString();
    }
}
