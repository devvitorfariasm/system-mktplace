-- Cópia local dos itens de cada pedido (consumo de OrderCreated): a reserva não consulta o order-service.
CREATE TABLE order_items_cache (
    order_id  UUID  NOT NULL,
    sku       TEXT  NOT NULL,
    quantity  INT   NOT NULL,
    PRIMARY KEY (order_id, sku)
);
