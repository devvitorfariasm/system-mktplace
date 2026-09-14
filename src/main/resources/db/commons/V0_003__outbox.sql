-- Transactional Outbox (ADR 0002): o evento é gravado na mesma transação do efeito de negócio;
-- o OutboxRelay publica em ordem de id (UUID v7 = ordem temporal) e marca published_at.
CREATE TABLE outbox (
    id             UUID        PRIMARY KEY,          -- vira o header eventId
    event_type     TEXT        NOT NULL,
    message_key    TEXT        NOT NULL,              -- chave de partição (orderId ou sku)
    order_id       UUID,
    correlation_id TEXT        NOT NULL,
    causation_id   TEXT,
    schema_version INT         NOT NULL DEFAULT 1,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT         NOT NULL DEFAULT 0,
    last_error     TEXT
);
-- O relay só lê pendentes: índice parcial pequeno mesmo com histórico grande
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
