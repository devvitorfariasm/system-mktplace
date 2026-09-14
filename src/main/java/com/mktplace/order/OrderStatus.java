package com.mktplace.order;

import java.util.Map;
import java.util.Set;

/**
 * Máquina de estados do pedido (seção 6.2). Qualquer transição fora dela é 409 na API
 * e "evento fora de ordem" no projetor (registrado no histórico, não aplicado).
 */
public enum OrderStatus {
    CREATED, PAID, CONFIRMED, PAYMENT_FAILED, OUT_OF_STOCK, CANCELLED;

    private static final Map<OrderStatus, Set<OrderStatus>> TRANSITIONS = Map.of(
            CREATED, Set.of(PAID, PAYMENT_FAILED, CANCELLED),
            PAID, Set.of(CONFIRMED, OUT_OF_STOCK, CANCELLED),
            PAYMENT_FAILED, Set.of(CREATED),      // retry
            OUT_OF_STOCK, Set.of(CREATED),        // retry
            CONFIRMED, Set.of(),
            CANCELLED, Set.of());

    public boolean canTransitionTo(OrderStatus next) {
        return TRANSITIONS.get(this).contains(next);
    }

    /** Terminal para a linha do tempo: o SSE emite "done" quando o pedido chega aqui. */
    public boolean isTerminal() {
        return this == CONFIRMED || this == CANCELLED || this == PAYMENT_FAILED || this == OUT_OF_STOCK;
    }
}
