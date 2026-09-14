# ADR 0003 · Como o audit log é capturado

**Status:** rascunho (fase 0) · implementação na fase 1

## Contexto
Cada evento precisa registrar quem produziu (tópico, partição, offset, headers) e cada tentativa de consumo (serviço, grupo, tempo de processamento, tentativa, status `SUCCESS/RETRY/DLQ`, erro). Opções: `ProducerInterceptor`/`RecordInterceptor`, Aspect, ou chamadas explícitas.

## Decisão
- **Produção: explícita em `EventPublisher`.** O `ProducerInterceptor.onAcknowledgement` recebe só `RecordMetadata`, sem headers nem payload; o `EventPublisher` tem tudo (record + metadata do `SendResult`) e emite um `AuditRecord(kind=PRODUCED)` no callback de sucesso. Todo produtor de negócio passa por ele, então nada escapa.
- **Consumo: `RecordInterceptor` do Spring Kafka.** `intercept()` marca o início e aplica o chaos; `success()`/`failure()` calculam `processingTimeMs`, leem `kafka_deliveryAttempt` e classificam `SUCCESS`, `RETRY` (exceção transitória com tentativas restantes) ou `DLQ` (última tentativa ou exceção não-retryable). Listeners não sabem que existe auditoria.
- Registros vão para `event-audit-log` (chave `orderId`). O `audit-service` os materializa em Postgres. O interceptor **ignora** `event-audit-log` e `*.DLT` para não auditar a auditoria.
- Sem Aspect: acoplaria a auditoria à assinatura dos métodos `@KafkaListener` e não veria retries feitos pelo container.

## Consequências
- A auditoria é assíncrona e "best effort": se o broker cair, o registro de auditoria se perde junto com o evento, o que é coerente.
- Um evento consumido por N grupos gera N+ registros; a tabela do audit-service é desnormalizada e indexada por `(order_id, produced_at)`.
- `DlqMessageDiscarded` e reprocessamentos também passam pelo mesmo canal, logo aparecem na linha do tempo.
