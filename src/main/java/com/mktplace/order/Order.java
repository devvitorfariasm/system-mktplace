package com.mktplace.order;

import com.mktplace.commons.web.ConflictException;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OrderColumn;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.jspecify.annotations.Nullable;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Domínio rico: a transição de status é validada aqui, sem Spring. */
@Entity
@Table(name = "orders")
public class Order {

    @Id
    private UUID id;
    @Column(name = "customer_id", nullable = false)
    private UUID customerId;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;
    @Column(nullable = false)
    private long total;
    @Column(nullable = false)
    private String currency;
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "shipping_address")
    private @Nullable Map<String, String> shippingAddress;
    private @Nullable String notes;
    @Column(name = "correlation_id", nullable = false)
    private String correlationId;
    @Version
    private long version;
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;
    @Column(name = "deleted_at")
    private @Nullable OffsetDateTime deletedAt;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "order_items", joinColumns = @JoinColumn(name = "order_id"))
    @OrderColumn(name = "position")
    private List<OrderItem> items = new ArrayList<>();

    protected Order() {
    }

    public Order(UUID id, UUID customerId, List<OrderItem> items, String currency, @Nullable Map<String, String> shippingAddress,
                 @Nullable String notes, String correlationId, OffsetDateTime now) {
        this.id = id;
        this.customerId = customerId;
        this.items = new ArrayList<>(items);
        this.total = items.stream().mapToLong(OrderItem::subtotal).sum();
        this.currency = currency;
        this.shippingAddress = shippingAddress;
        this.notes = notes;
        this.correlationId = correlationId;
        this.status = OrderStatus.CREATED;
        this.createdAt = now;
        this.updatedAt = now;
    }

    /** Aplica a transição ou lança 409 (uso pela API: cancel, retry). */
    public void transitionTo(OrderStatus next, OffsetDateTime now) {
        if (!status.canTransitionTo(next)) {
            throw new ConflictException("invalid-status-transition",
                    "Pedido %s está %s e não pode ir para %s".formatted(id, status, next));
        }
        this.status = next;
        this.updatedAt = now;
    }

    /** Versão silenciosa para o projetor: devolve false quando o evento chegou fora de ordem. */
    public boolean tryTransitionTo(OrderStatus next, OffsetDateTime now) {
        if (!status.canTransitionTo(next)) {
            return false;
        }
        this.status = next;
        this.updatedAt = now;
        return true;
    }

    public UUID getId() {
        return id;
    }

    public UUID getCustomerId() {
        return customerId;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public long getTotal() {
        return total;
    }

    public String getCurrency() {
        return currency;
    }

    public @Nullable Map<String, String> getShippingAddress() {
        return shippingAddress;
    }

    public @Nullable String getNotes() {
        return notes;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public long getVersion() {
        return version;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    public @Nullable OffsetDateTime getDeletedAt() {
        return deletedAt;
    }

    public List<OrderItem> getItems() {
        return List.copyOf(items);
    }
}
