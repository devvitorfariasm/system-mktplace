package com.mktplace.order;

import com.mktplace.commons.web.ConflictException;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderStatusTest {

    private static Order order() {
        return new Order(UUID.randomUUID(), UUID.randomUUID(), List.of(new OrderItem("SKU-1", 2, 100), new OrderItem("SKU-2", 1, 50)),
                "BRL", null, null, "corr", OffsetDateTime.now());
    }

    @Test
    void totalEhASomaDosSubtotaisEmCentavos() {
        assertThat(order().getTotal()).isEqualTo(250);
    }

    @Test
    void caminhoFelizCreatedPaidConfirmed() {
        Order o = order();
        assertThat(o.tryTransitionTo(OrderStatus.PAID, OffsetDateTime.now())).isTrue();
        assertThat(o.tryTransitionTo(OrderStatus.CONFIRMED, OffsetDateTime.now())).isTrue();
        assertThat(o.getStatus().isTerminal()).isTrue();
    }

    @Test
    void eventoForaDeOrdemNaoMudaOStatus() {
        Order o = order();
        o.tryTransitionTo(OrderStatus.PAID, OffsetDateTime.now());
        o.tryTransitionTo(OrderStatus.CONFIRMED, OffsetDateTime.now());
        assertThat(o.tryTransitionTo(OrderStatus.PAYMENT_FAILED, OffsetDateTime.now())).isFalse();
        assertThat(o.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    }

    @Test
    void transicaoInvalidaPelaApiEh409() {
        Order o = order();
        o.tryTransitionTo(OrderStatus.PAID, OffsetDateTime.now());
        o.tryTransitionTo(OrderStatus.CONFIRMED, OffsetDateTime.now());
        assertThatThrownBy(() -> o.transitionTo(OrderStatus.CANCELLED, OffsetDateTime.now()))
                .isInstanceOf(ConflictException.class);
    }

    @Test
    void retrySoDePaymentFailedOuOutOfStock() {
        assertThat(OrderStatus.PAYMENT_FAILED.canTransitionTo(OrderStatus.CREATED)).isTrue();
        assertThat(OrderStatus.OUT_OF_STOCK.canTransitionTo(OrderStatus.CREATED)).isTrue();
        assertThat(OrderStatus.CONFIRMED.canTransitionTo(OrderStatus.CREATED)).isFalse();
        assertThat(OrderStatus.PAID.canTransitionTo(OrderStatus.CANCELLED)).isTrue();
    }
}
