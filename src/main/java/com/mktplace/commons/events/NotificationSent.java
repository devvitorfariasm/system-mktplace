package com.mktplace.commons.events;

import java.time.OffsetDateTime;
import java.util.UUID;

/** Notificação registrada para o cliente, em resposta a um evento terminal. */
public record NotificationSent(
        UUID orderId,
        UUID notificationId,
        UUID customerId,
        String channel,
        String triggeredBy,
        OffsetDateTime sentAt) implements DomainEvent {

    @Override
    public String eventType() {
        return EventTypes.NOTIFICATION_SENT;
    }

    @Override
    public String key() {
        return orderId.toString();
    }
}
