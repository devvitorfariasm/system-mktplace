# ADR 0002 · Outbox: relay agendado em vez de CDC (Debezium)

**Status:** aceito (fase 2)

## Contexto
`order-service` grava o pedido no PostgreSQL e publica `OrderCreated`. Duas operações desconectadas podem deixar pedido sem evento (commit ok, Kafka falhou) ou evento sem pedido (Kafka ok, rollback). O desafio exige Transactional Outbox.

## Decisão
- Tabela `outbox(id, aggregate_id, event_type, headers jsonb, payload jsonb, created_at, published_at, attempts)` gravada **na mesma transação** do pedido.
- Um relay `@Scheduled` (a cada 500 ms) lê lotes `WHERE published_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED`, publica com `EventPublisher` (acks=all, idempotente), e marca `published_at`. Falha → `attempts++` e nova tentativa no próximo ciclo.
- O outbox vive no pacote `commons` e **todos** os serviços publicam por ele (`OutboxPublisher.enqueue` dentro da transação do listener). O desafio só exige no order-service, mas o custo é zero e elimina a perda de evento entre commit e publicação em qualquer serviço.
- Sem Debezium: exigiria conector, Kafka Connect e WAL logical decoding no compose; para o escopo do desafio o custo operacional não paga o ganho de latência.

## Consequências
- Entrega **pelo menos uma vez**: se o processo cair entre `send` e `UPDATE`, o evento é republicado com o mesmo `eventId`. Por isso todo consumidor é idempotente (`processed_events`).
- Latência de publicação de até ~500 ms (visível na linha do tempo como "gap" entre `createdAt` e `producedAt`).
- `SKIP LOCKED` permite mais de uma instância do order-service sem duplicar trabalho.
- Com mais tempo: Debezium com outbox event router elimina o polling e mantém a mesma tabela.
