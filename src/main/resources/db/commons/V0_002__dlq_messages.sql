-- Materialização da(s) DLT deste serviço: alimenta GET /admin/dlq, reprocessamento e descarte.
CREATE TABLE dlq_messages (
    id                 UUID        PRIMARY KEY,
    event_id           UUID,
    event_type         TEXT,
    message_key        TEXT,
    original_topic     TEXT        NOT NULL,
    original_partition INT         NOT NULL,
    original_offset    BIGINT      NOT NULL,
    consumer_group     TEXT        NOT NULL,
    dlt_topic          TEXT        NOT NULL,
    dlt_partition      INT         NOT NULL,
    dlt_offset         BIGINT      NOT NULL,
    exception_class    TEXT,
    exception_message  TEXT,
    stack_trace        TEXT,
    attempts           INT         NOT NULL DEFAULT 1,
    headers            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    payload            TEXT,
    failed_at          TIMESTAMPTZ NOT NULL,
    status             TEXT        NOT NULL DEFAULT 'PENDING',   -- PENDING | REPROCESSED | DISCARDED
    resolved_at        TIMESTAMPTZ,
    resolution_reason  TEXT,
    reprocess_count    INT         NOT NULL DEFAULT 0,
    UNIQUE (dlt_topic, dlt_partition, dlt_offset)
);
-- Listagem padrão: pendentes, mais recentes primeiro
CREATE INDEX dlq_messages_status_failed_at_idx ON dlq_messages (status, failed_at DESC);
CREATE INDEX dlq_messages_event_id_idx ON dlq_messages (event_id);
