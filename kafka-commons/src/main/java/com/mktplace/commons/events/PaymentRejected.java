package com.mktplace.commons.events;

import java.time.OffsetDateTime;
import java.util.UUID;

public record PaymentRejected(
        UUID orderId,
        UUID paymentId,
        long amount,
        String currency,
        String reason,
        OffsetDateTime rejectedAt) implements DomainEvent {

    @Override
    public String eventType() {
        return EventTypes.PAYMENT_REJECTED;
    }

    @Override
    public String key() {
        return orderId.toString();
    }
}
