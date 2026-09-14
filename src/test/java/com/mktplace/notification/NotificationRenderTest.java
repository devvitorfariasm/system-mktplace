package com.mktplace.notification;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationRenderTest {

    @Test
    void substituiPlaceholdersEDeixaVazioOsDesconhecidos() {
        String out = NotificationListener.render("Pedido {{orderId}} de {{total}} ({{nada}})", Map.of("orderId", "abc", "total", "10"));
        assertThat(out).isEqualTo("Pedido abc de 10 ()");
    }
}
