package com.mktplace.commons.events;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Pedido cancelado. Se estava PAID, {@code refundRequired=true} dispara estorno e liberação de estoque.
 */
public record OrderCancelled(
        UUID orderId,
        String previousStatus,
        boolean refundRequired,
        String reason,
        OffsetDateTime cancelledAt) implements DomainEvent {

    @Override
    public String eventType() {
        return EventTypes.ORDER_CANCELLED;
    }

    @Override
    public String key() {
        return orderId.toString();
    }
}
