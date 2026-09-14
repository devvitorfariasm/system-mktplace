-- notification-service: registro das notificações disparadas por eventos terminais.
CREATE TABLE notifications (
    id                UUID        PRIMARY KEY,
    order_id          UUID        NOT NULL,
    customer_id       UUID,
    channel           TEXT        NOT NULL,        -- EMAIL | SMS | PUSH
    status            TEXT        NOT NULL,        -- SENT | FAILED | PENDING
    event_type        TEXT        NOT NULL,        -- evento que disparou
    triggered_by_event_id TEXT    NOT NULL,
    subject           TEXT        NOT NULL,
    body              TEXT        NOT NULL,
    sent_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    version           BIGINT      NOT NULL DEFAULT 0
);
CREATE INDEX notifications_order_idx ON notifications (order_id);
CREATE INDEX notifications_customer_idx ON notifications (customer_id);

CREATE TABLE notification_templates (
    event_type TEXT PRIMARY KEY,
    subject    TEXT NOT NULL,
    body       TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO notification_templates (event_type, subject, body) VALUES
 ('StockReserved',   'Pedido {{orderId}} confirmado',            'Seu pedido {{orderId}} no valor de {{total}} foi confirmado e está sendo preparado.'),
 ('PaymentRejected', 'Pagamento do pedido {{orderId}} recusado', 'Não conseguimos aprovar o pagamento do pedido {{orderId}}: {{reason}}.'),
 ('StockUnavailable','Pedido {{orderId}} sem estoque',           'Um ou mais itens do pedido {{orderId}} estão sem estoque. Você pode tentar novamente mais tarde.');
