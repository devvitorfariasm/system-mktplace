-- inventory-service: produtos com saldo total/reservado, reservas por pedido e movimentos de estoque.
CREATE TABLE products (
    sku        TEXT        PRIMARY KEY,
    name       TEXT        NOT NULL,
    price      BIGINT      NOT NULL CHECK (price >= 0),           -- centavos
    total      INT         NOT NULL DEFAULT 0 CHECK (total >= 0),
    reserved   INT         NOT NULL DEFAULT 0 CHECK (reserved >= 0 AND reserved <= total),
    active     BOOLEAN     NOT NULL DEFAULT TRUE,
    version    BIGINT      NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- available = total - reserved é calculado no banco (nunca em Java); o CHECK acima impede overselling

CREATE TABLE reservations (
    id          UUID        PRIMARY KEY,
    order_id    UUID        NOT NULL,
    sku         TEXT        NOT NULL REFERENCES products (sku),
    quantity    INT         NOT NULL CHECK (quantity > 0),
    status      TEXT        NOT NULL,                                -- ACTIVE | RELEASED | CONSUMED
    event_id    TEXT        NOT NULL,                                -- PaymentApproved que originou
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ,
    UNIQUE (order_id, sku)
);
CREATE INDEX reservations_order_idx ON reservations (order_id);
CREATE INDEX reservations_status_idx ON reservations (status);

CREATE TABLE stock_movements (
    id                 UUID        PRIMARY KEY,
    sku                TEXT        NOT NULL REFERENCES products (sku),
    order_id           UUID,
    delta              INT         NOT NULL,                        -- reserva = -qtd em available, ajuste = ±n em total
    kind               TEXT        NOT NULL,                        -- RESERVE | RELEASE | ADJUST | INITIAL
    reason             TEXT        NOT NULL,
    caused_by_event_id TEXT,
    balance_total      INT         NOT NULL,
    balance_reserved   INT         NOT NULL,
    occurred_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX stock_movements_sku_idx ON stock_movements (sku, occurred_at DESC);
