package com.mktplace.order;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

/** Item do pedido; preço congelado no momento da criação (mudança de preço não afeta pedidos existentes). */
@Embeddable
public record OrderItem(
        @Column(nullable = false) String sku,
        @Column(nullable = false) int quantity,
        @Column(name = "unit_price", nullable = false) long unitPrice) {

    public long subtotal() {
        return (long) quantity * unitPrice;
    }
}
