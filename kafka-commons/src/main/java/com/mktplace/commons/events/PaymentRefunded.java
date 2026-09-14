package com.mktplace.commons.events;

import java.time.OffsetDateTime;
import java.util.UUID;

public record PaymentRefunded(
        UUID orderId,
        UUID paymentId,
        long amount,
        String currency,
        String reason,
        OffsetDateTime refundedAt) implements DomainEvent {

    @Override
    public String eventType() {
        return EventTypes.PAYMENT_REFUNDED;
    }

    @Override
    public String key() {
        return orderId.toString();
    }
}
