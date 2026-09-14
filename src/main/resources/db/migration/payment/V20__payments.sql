-- payment-service: pagamentos nascem só do consumo de OrderCreated; cada tentativa simulada fica registrada.
CREATE TABLE payments (
    id           UUID        PRIMARY KEY,
    order_id     UUID        NOT NULL,
    amount       BIGINT      NOT NULL,
    currency     TEXT        NOT NULL,
    status       TEXT        NOT NULL,             -- APPROVED | REJECTED | REFUNDED
    reason       TEXT,
    latency_ms   BIGINT      NOT NULL,
    event_id     TEXT        NOT NULL,             -- OrderCreated que originou
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    version      BIGINT      NOT NULL DEFAULT 0
);
CREATE INDEX payments_order_idx ON payments (order_id);
CREATE INDEX payments_status_processed_idx ON payments (status, processed_at DESC);

CREATE TABLE payment_attempts (
    id         UUID        PRIMARY KEY,
    payment_id UUID        NOT NULL REFERENCES payments (id) ON DELETE CASCADE,
    attempt    INT         NOT NULL,
    result     TEXT        NOT NULL,
    latency_ms BIGINT      NOT NULL,
    reason     TEXT,
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
