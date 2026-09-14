package com.mktplace.commons.events;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/** Reservas liberadas (cancelamento, estorno ou operação manual). */
public record StockReleased(
        UUID orderId,
        List<UUID> reservationIds,
        String reason,
        OffsetDateTime releasedAt) implements DomainEvent {

    @Override
    public String eventType() {
        return EventTypes.STOCK_RELEASED;
    }

    @Override
    public String key() {
        return orderId.toString();
    }
}
