package com.mktplace.commons.events;

import java.time.OffsetDateTime;
import java.util.UUID;

public record PaymentApproved(
        UUID orderId,
        UUID paymentId,
        long amount,
        String currency,
        OffsetDateTime approvedAt) implements DomainEvent {

    @Override
    public String eventType() {
        return EventTypes.PAYMENT_APPROVED;
    }

    @Override
    public String key() {
        return orderId.toString();
    }
}
