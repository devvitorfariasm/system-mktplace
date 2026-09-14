-- audit-service: materialização desnormalizada de event-audit-log, pronta para a linha do tempo.
CREATE TABLE audit_events (
    id                 UUID        PRIMARY KEY,                -- recordId do AuditRecord (idempotente)
    kind               TEXT        NOT NULL,                   -- PRODUCED | CONSUMED | DLQ_DISCARDED
    service            TEXT        NOT NULL,
    event_id           TEXT,
    event_type         TEXT,
    topic              TEXT        NOT NULL,
    partition          INT,
    kafka_offset       BIGINT,
    message_key        TEXT,
    order_id           UUID,
    correlation_id     TEXT,
    causation_id       TEXT,
    schema_version     INT,
    producer           TEXT,
    produced_at        TIMESTAMPTZ,
    headers            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    payload            JSONB,                                  -- parseado quando é JSON válido
    payload_raw        TEXT,
    consumer_group     TEXT,
    consumed_at        TIMESTAMPTZ,
    processing_time_ms BIGINT,
    attempt            INT,
    status             TEXT,                                   -- SUCCESS | RETRY | DLQ
    error_class        TEXT,
    error_message      TEXT,
    recorded_at        TIMESTAMPTZ NOT NULL
);
-- A linha do tempo de um pedido é uma única query por (order_id, produced_at)
CREATE INDEX audit_events_order_produced_idx ON audit_events (order_id, produced_at, id);
CREATE INDEX audit_events_event_id_idx ON audit_events (event_id);
CREATE INDEX audit_events_correlation_idx ON audit_events (correlation_id);
CREATE INDEX audit_events_causation_idx ON audit_events (causation_id);
-- Busca global com cursor obrigatoriamente em (produced_at, id)
CREATE INDEX audit_events_produced_idx ON audit_events (produced_at DESC, id DESC);
CREATE INDEX audit_events_topic_partition_offset_idx ON audit_events (topic, partition, kafka_offset);
CREATE INDEX audit_events_payload_gin_idx ON audit_events USING GIN (payload jsonb_path_ops);
