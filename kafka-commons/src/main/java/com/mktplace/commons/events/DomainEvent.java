package com.mktplace.commons.events;

import org.jspecify.annotations.Nullable;

import java.util.UUID;

/**
 * Contrato de todo evento de negócio. Os records são o schema: mudar um campo exige versionar
 * (docs/eventos.md). Os métodos abaixo não são componentes de record, então não entram no JSON.
 */
public sealed interface DomainEvent permits
        OrderCreated, OrderCancelled,
        PaymentApproved, PaymentRejected, PaymentRefunded,
        StockReserved, StockUnavailable, StockReleased, StockAdjusted,
        NotificationSent {

    String eventType();

    /** Chave de partição: orderId no fluxo do pedido; sku no ajuste de estoque. */
    String key();

    /** Pedido relacionado, quando houver (vai no header orderId e na chave de auditoria). */
    @Nullable
    UUID orderId();

    default int schemaVersion() {
        return 1;
    }
}
