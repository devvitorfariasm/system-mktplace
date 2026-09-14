package com.mktplace.order;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.jspecify.annotations.Nullable;

import java.time.OffsetDateTime;
import java.util.UUID;

/** Append-only: cada tentativa de transição, aplicada ou ignorada (evento fora de ordem). */
@Entity
@Table(name = "order_status_history")
public class OrderStatusHistory {

    @Id
    private UUID id;
    @Column(name = "order_id", nullable = false)
    private UUID orderId;
    @Enumerated(EnumType.STRING)
    @Column(name = "from_status")
    private @Nullable OrderStatus fromStatus;
    @Enumerated(EnumType.STRING)
    @Column(name = "to_status", nullable = false)
    private OrderStatus toStatus;
    @Column(name = "event_id")
    private @Nullable String eventId;
    @Column(name = "event_type")
    private @Nullable String eventType;
    @Column(nullable = false)
    private boolean applied;
    private @Nullable String reason;
    @Column(name = "occurred_at", nullable = false)
    private OffsetDateTime occurredAt;

    protected OrderStatusHistory() {
    }

    public OrderStatusHistory(UUID id, UUID orderId, @Nullable OrderStatus from, OrderStatus to, @Nullable String eventId,
                              @Nullable String eventType, boolean applied, @Nullable String reason, OffsetDateTime at) {
        this.id = id;
        this.orderId = orderId;
        this.fromStatus = from;
        this.toStatus = to;
        this.eventId = eventId;
        this.eventType = eventType;
        this.applied = applied;
        this.reason = reason;
        this.occurredAt = at;
    }

    public @Nullable OrderStatus getFromStatus() {
        return fromStatus;
    }

    public OrderStatus getToStatus() {
        return toStatus;
    }

    public @Nullable String getEventId() {
        return eventId;
    }

    public @Nullable String getEventType() {
        return eventType;
    }

    public boolean isApplied() {
        return applied;
    }

    public @Nullable String getReason() {
        return reason;
    }

    public OffsetDateTime getOccurredAt() {
        return occurredAt;
    }
}
