package com.mktplace.inventory;

import com.mktplace.commons.events.OrderCreated;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/** Tabela order_items_cache (V31): itens do pedido conhecidos pelo inventory-service. */
@Repository
@Profile("inventory")
public class OrderItemsCache {

    private final JdbcClient jdbc;

    public OrderItemsCache(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void save(OrderCreated order) {
        for (OrderCreated.Item item : order.items()) {
            jdbc.sql("INSERT INTO order_items_cache (order_id, sku, quantity) VALUES (:orderId, :sku, :qty) ON CONFLICT DO NOTHING")
                    .param("orderId", order.orderId()).param("sku", item.sku()).param("qty", item.quantity()).update();
        }
    }

    public List<OrderCreated.Item> find(UUID orderId) {
        var items = jdbc.sql("SELECT sku, quantity FROM order_items_cache WHERE order_id = :orderId ORDER BY sku")
                .param("orderId", orderId)
                .query((rs, i) -> new OrderCreated.Item(rs.getString("sku"), rs.getInt("quantity"), 0))
                .list();
        if (items.isEmpty()) {
            // OrderCreated ainda não chegou neste serviço (ordem entre tópicos não é garantida): retry com backoff
            throw new com.mktplace.commons.consumer.TransientFailureException("Itens do pedido " + orderId + " ainda não conhecidos");
        }
        return items;
    }
}
