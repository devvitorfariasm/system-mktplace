-- Idempotência do consumer: (evento, grupo) processado uma única vez.
-- O INSERT acontece na mesma transação do efeito de negócio; reprocessar a mesma mensagem não gera efeito duplicado.
CREATE TABLE processed_events (
    event_id       UUID        NOT NULL,
    consumer_group TEXT        NOT NULL,
    topic          TEXT        NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_group)
);
