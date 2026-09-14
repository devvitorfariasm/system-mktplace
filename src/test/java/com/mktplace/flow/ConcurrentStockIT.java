package com.mktplace.flow;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ConcurrentStockIT extends AbstractFlowIT {

    @Test
    void doisPedidosConcorrentesParaEstoqueUmGeramExatamenteUmConfirmadoEUmSemEstoque() {
        UUID customer = newCustomer();
        String sku = newProduct(1);

        CompletableFuture<Reply> first = CompletableFuture.supplyAsync(() -> createOrder(customer, sku, 1, UUID.randomUUID().toString()));
        CompletableFuture<Reply> second = CompletableFuture.supplyAsync(() -> createOrder(customer, sku, 1, UUID.randomUUID().toString()));
        List<String> ids = List.of(first.join().text("id"), second.join().text("id"));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            for (String id : ids) {
                assertThat(get("/api/v1/orders/" + id).body().get("finished").asBoolean()).isTrue();
            }
        });
        Set<String> statuses = new java.util.HashSet<>();
        for (String id : ids) {
            statuses.add(get("/api/v1/orders/" + id).text("status"));
        }
        assertThat(statuses).containsExactlyInAnyOrder("CONFIRMED", "OUT_OF_STOCK");

        Reply product = get("/api/v1/products/" + sku);
        assertThat(product.body().get("available").asInt()).isZero();
        assertThat(product.body().get("reserved").asInt()).isEqualTo(1);

        String outOfStock = ids.stream().filter(id -> get("/api/v1/orders/" + id).text("status").equals("OUT_OF_STOCK")).findFirst().orElseThrow();
        await().atMost(TIMEOUT).untilAsserted(() -> {
            Reply timeline = get("/api/v1/orders/" + outOfStock + "/timeline");
            assertThat(timeline.text("currentStatus")).isEqualTo("OUT_OF_STOCK");
            var unavailable = timeline.body().get("events").get(2);
            assertThat(unavailable.get("eventType").asString()).isEqualTo("StockUnavailable");
            assertThat(unavailable.get("payload").get("shortages").get(0).get("available").asInt()).isZero();
        });
    }
}
