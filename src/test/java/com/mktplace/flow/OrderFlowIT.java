package com.mktplace.flow;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class OrderFlowIT extends AbstractFlowIT {

    @Test
    void pedidoAtravessaOsQuatroServicosAteConfirmadoComLinhaDoTempoCompleta() {
        UUID customer = newCustomer();
        String sku = newProduct(10);
        String key = "idem-" + UUID.randomUUID();

        Reply created = createOrder(customer, sku, 2, key);
        assertThat(created.status()).isEqualTo(201);
        assertThat(created.headers()).containsKey("location").containsKey("etag");
        assertThat(created.text("status")).isEqualTo("CREATED");
        assertThat(created.body().get("total").asLong()).isEqualTo(9980);
        assertThat(created.headers().get("x-correlation-id")).isEqualTo(created.text("correlationId"));
        String orderId = created.text("id");

        // Idempotency-Key: mesmo corpo → 200 + Idempotent-Replayed; corpo diferente → 422
        Reply replayed = createOrder(customer, sku, 2, key);
        assertThat(replayed.status()).isEqualTo(200);
        assertThat(replayed.headers().get("idempotent-replayed")).isEqualTo("true");
        assertThat(replayed.text("id")).isEqualTo(orderId);
        Reply conflict = createOrder(customer, sku, 3, key);
        assertThat(conflict.status()).isEqualTo(422);
        assertThat(conflict.text("code")).isEqualTo("idempotency-key-reused");

        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(get("/api/v1/orders/" + orderId).text("status")).isEqualTo("CONFIRMED"));

        Reply order = get("/api/v1/orders/" + orderId);
        // transições aplicadas em ordem; um PaymentApproved atrasado pode aparecer como não aplicado, e tudo bem
        List<String> history = new ArrayList<>();
        order.body().get("statusHistory").forEach(h -> {
            if (h.get("applied").asBoolean()) {
                history.add(h.get("to").asString());
            }
        });
        assertThat(history).containsExactly("CREATED", "PAID", "CONFIRMED");
        assertThat(order.body().get("finished").asBoolean()).isTrue();

        Reply product = get("/api/v1/products/" + sku);
        assertThat(product.body().get("reserved").asInt()).isEqualTo(2);
        assertThat(product.body().get("available").asInt()).isEqualTo(8);

        // a linha do tempo é eventualmente consistente com o coletor de auditoria
        await().atMost(TIMEOUT).untilAsserted(() -> {
            Reply timeline = get("/api/v1/orders/" + orderId + "/timeline");
            assertThat(timeline.status()).isEqualTo(200);
            List<String> types = new ArrayList<>();
            timeline.body().get("events").forEach(e -> types.add(e.get("eventType").asString()));
            assertThat(types).containsExactly("OrderCreated", "PaymentApproved", "StockReserved", "NotificationSent");
            assertThat(timeline.text("currentStatus")).isEqualTo("CONFIRMED");
            assertThat(timeline.text("correlationId")).isEqualTo(created.text("correlationId"));
            assertThat(timeline.body().get("dlq")).isEmpty();
            assertThat(timeline.body().get("totalDurationMs").asLong()).isGreaterThanOrEqualTo(0);

            JsonNode orderCreated = timeline.body().get("events").get(0);
            JsonNode paymentApproved = timeline.body().get("events").get(1);
            JsonNode stockReserved = timeline.body().get("events").get(2);
            assertThat(orderCreated.get("producer").asString()).isEqualTo("test-service");
            assertThat(orderCreated.get("key").asString()).isEqualTo(orderId);
            assertThat(orderCreated.get("causationId").isNull()).isTrue();
            assertThat(paymentApproved.get("causationId").asString()).isEqualTo(orderCreated.get("eventId").asString());
            assertThat(stockReserved.get("causationId").asString()).isEqualTo(paymentApproved.get("eventId").asString());
            assertThat(orderCreated.get("headers").has("schemaVersion")).isTrue();
            assertThat(orderCreated.get("payload").get("total").asLong()).isEqualTo(9980);

            List<String> groups = new ArrayList<>();
            orderCreated.get("consumers").forEach(c -> groups.add(c.get("consumerGroup").asString()));
            assertThat(groups).contains("payment-service-v1", "inventory-service-orders-v1");
            JsonNode inventoryConsumer = null;
            for (JsonNode c : paymentApproved.get("consumers")) {
                if (c.get("consumerGroup").asString().equals("inventory-service-v1")) {
                    inventoryConsumer = c;
                }
            }
            assertThat(inventoryConsumer).isNotNull();
            assertThat(inventoryConsumer.get("attempts").get(0).get("status").asString()).isEqualTo("SUCCESS");
            assertThat(inventoryConsumer.get("emitted").get(0).asString()).isEqualTo(stockReserved.get("eventId").asString());
            assertThat(timeline.body().get("gaps").size()).isEqualTo(3);
        });
    }

    @Test
    void validacoesDeCriacao() {
        UUID customer = newCustomer();
        Reply semChave = call(HttpMethod.POST, "/api/v1/orders", Map.of("customerId", customer, "currency", "BRL",
                "items", List.of(Map.of("sku", "X", "quantity", 1, "unitPrice", 1))), Map.of());
        assertThat(semChave.status()).isEqualTo(400);

        Reply duplicado = call(HttpMethod.POST, "/api/v1/orders", Map.of("customerId", customer, "currency", "BRL",
                "items", List.of(Map.of("sku", "X", "quantity", 1, "unitPrice", 1), Map.of("sku", "X", "quantity", 1, "unitPrice", 1))),
                Map.of("Idempotency-Key", UUID.randomUUID().toString()));
        assertThat(duplicado.status()).isEqualTo(422);
        assertThat(duplicado.body().get("errors").get(0).get("field").asString()).isEqualTo("items[1].sku");

        Reply clienteInexistente = createOrder(UUID.randomUUID(), "X", 1, UUID.randomUUID().toString());
        assertThat(clienteInexistente.status()).isEqualTo(422);
        assertThat(clienteInexistente.body().get("errors").get(0).get("field").asString()).isEqualTo("customerId");

        Reply quantidadeZero = call(HttpMethod.POST, "/api/v1/orders", Map.of("customerId", customer, "currency", "BRL",
                "items", List.of(Map.of("sku", "X", "quantity", 0, "unitPrice", 1))), Map.of("Idempotency-Key", UUID.randomUUID().toString()));
        assertThat(quantidadeZero.status()).isEqualTo(400);
        assertThat(quantidadeZero.body().get("errors").get(0).get("field").asString()).isEqualTo("items[0].quantity");

        assertThat(get("/api/v1/orders/" + UUID.randomUUID()).status()).isEqualTo(404);
    }
}
