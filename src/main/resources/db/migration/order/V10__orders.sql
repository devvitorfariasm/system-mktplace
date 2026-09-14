-- order-service: clientes, pedidos, itens, histórico de status e chaves de idempotência.
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE customers (
    id         UUID        PRIMARY KEY,
    name       TEXT        NOT NULL,
    email      CITEXT      NOT NULL UNIQUE,        -- case-insensitive
    version    BIGINT      NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    anonymized_at TIMESTAMPTZ
);

CREATE TABLE orders (
    id               UUID        PRIMARY KEY,           -- UUID v7, também é a chave Kafka
    customer_id      UUID        NOT NULL REFERENCES customers (id),
    status           TEXT        NOT NULL,
    total            BIGINT      NOT NULL,              -- centavos
    currency         TEXT        NOT NULL,
    shipping_address JSONB,
    notes            TEXT,
    correlation_id   TEXT        NOT NULL,
    version          BIGINT      NOT NULL DEFAULT 0,    -- lock otimista / ETag
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at       TIMESTAMPTZ
);
-- listagem padrão: mais recentes primeiro, com cursor (created_at, id) e filtro por status
CREATE INDEX orders_created_at_id_idx ON orders (created_at DESC, id DESC) WHERE deleted_at IS NULL;
CREATE INDEX orders_status_idx ON orders (status) WHERE deleted_at IS NULL;
CREATE INDEX orders_customer_idx ON orders (customer_id);

CREATE TABLE order_items (
    order_id   UUID   NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    position   INT    NOT NULL,
    sku        TEXT   NOT NULL,
    quantity   INT    NOT NULL CHECK (quantity > 0),
    unit_price BIGINT NOT NULL CHECK (unit_price >= 0),
    PRIMARY KEY (order_id, position),
    UNIQUE (order_id, sku)                               -- item duplicado é 422 na API e impossível no banco
);

CREATE TABLE order_status_history (
    id          UUID        PRIMARY KEY,
    order_id    UUID        NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    from_status TEXT,
    to_status   TEXT        NOT NULL,
    event_id    TEXT,
    event_type  TEXT,
    applied     BOOLEAN     NOT NULL DEFAULT TRUE,       -- FALSE = evento fora de ordem, registrado e ignorado
    reason      TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX order_status_history_order_idx ON order_status_history (order_id, occurred_at);

-- Idempotency-Key de POST /orders: mesma chave + mesmo corpo → resposta original; corpo diferente → 422
CREATE TABLE idempotency_keys (
    idem_key      TEXT        PRIMARY KEY,
    request_hash  TEXT        NOT NULL,
    order_id      UUID        NOT NULL,
    response_body JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
