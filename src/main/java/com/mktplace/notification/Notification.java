package com.mktplace.notification;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.jspecify.annotations.Nullable;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "notifications")
public class Notification {

    @Id
    private UUID id;
    @Column(name = "order_id", nullable = false)
    private UUID orderId;
    @Column(name = "customer_id")
    private @Nullable UUID customerId;
    @Column(nullable = false)
    private String channel;
    @Column(nullable = false)
    private String status;
    @Column(name = "event_type", nullable = false)
    private String eventType;
    @Column(name = "triggered_by_event_id", nullable = false)
    private String triggeredByEventId;
    @Column(nullable = false)
    private String subject;
    @Column(nullable = false)
    private String body;
    @Column(name = "sent_at", nullable = false)
    private OffsetDateTime sentAt;
    @Version
    private long version;

    protected Notification() {
    }

    public Notification(UUID id, UUID orderId, @Nullable UUID customerId, String channel, String status, String eventType,
                        String triggeredByEventId, String subject, String body, OffsetDateTime sentAt) {
        this.id = id;
        this.orderId = orderId;
        this.customerId = customerId;
        this.channel = channel;
        this.status = status;
        this.eventType = eventType;
        this.triggeredByEventId = triggeredByEventId;
        this.subject = subject;
        this.body = body;
        this.sentAt = sentAt;
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getChannel() {
        return channel;
    }

    public String getEventType() {
        return eventType;
    }

    public String getSubject() {
        return subject;
    }

    public String getBody() {
        return body;
    }
}
