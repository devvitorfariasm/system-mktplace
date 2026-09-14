package com.mktplace.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.jspecify.annotations.Nullable;

import java.time.OffsetDateTime;
import java.util.UUID;

/** Pagamento: nasce apenas do consumo de OrderCreated (não há POST /payments público). */
@Entity
@Table(name = "payments")
public class Payment {

    public enum Status { APPROVED, REJECTED, REFUNDED }

    @Id
    private UUID id;
    @Column(name = "order_id", nullable = false)
    private UUID orderId;
    @Column(nullable = false)
    private long amount;
    @Column(nullable = false)
    private String currency;
    @jakarta.persistence.Enumerated(jakarta.persistence.EnumType.STRING)
    @Column(nullable = false)
    private Status status;
    private @Nullable String reason;
    @Column(name = "latency_ms", nullable = false)
    private long latencyMs;
    @Column(name = "event_id", nullable = false)
    private String eventId;
    @Column(name = "processed_at", nullable = false)
    private OffsetDateTime processedAt;
    @Version
    private long version;

    protected Payment() {
    }

    public Payment(UUID id, UUID orderId, long amount, String currency, Status status, @Nullable String reason,
                   long latencyMs, String eventId, OffsetDateTime processedAt) {
        this.id = id;
        this.orderId = orderId;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.reason = reason;
        this.latencyMs = latencyMs;
        this.eventId = eventId;
        this.processedAt = processedAt;
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public long getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public Status getStatus() {
        return status;
    }

    public @Nullable String getReason() {
        return reason;
    }

    public long getLatencyMs() {
        return latencyMs;
    }

    public OffsetDateTime getProcessedAt() {
        return processedAt;
    }
}
