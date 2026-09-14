# Contrato de eventos Kafka

Fonte da verdade: `src/main/java/com/mktplace/commons/events/` (`Topics`, `EventHeaders`, records de evento).
Este documento explica as decisões; o código é o contrato.

## Tópicos

Um tópico por tipo de evento, **3 partições**, replicação 1 (cluster de um nó). Todo tópico de negócio tem o irmão `<topic>.DLT` com o mesmo número de partições, para que a mensagem morta preserve a partição original.

| Tópico | Evento | Produtor | Consumidores (consumer group) | Chave |
|---|---|---|---|---|
| `orders.created` | `OrderCreated` | order-service (via outbox) | payment-service (`payment-service-v1`), order-service (`order-status-projector`) | `orderId` |
| `orders.cancelled` | `OrderCancelled` | order-service | payment-service, inventory-service, notification-service | `orderId` |
| `payments.approved` | `PaymentApproved` | payment-service | inventory-service (`inventory-service-v1`), order-service | `orderId` |
| `payments.rejected` | `PaymentRejected` | payment-service | notification-service (`notification-service-v1`), order-service | `orderId` |
| `payments.refunded` | `PaymentRefunded` | payment-service | order-service, notification-service | `orderId` |
| `inventory.reserved` | `StockReserved` | inventory-service | notification-service, order-service | `orderId` |
| `inventory.unavailable` | `StockUnavailable` | inventory-service | notification-service, order-service | `orderId` |
| `inventory.released` | `StockReleased` | inventory-service | order-service | `orderId` |
| `inventory.adjusted` | `StockAdjusted` | inventory-service | audit (somente rastreio) | `sku` (evento operacional, sem pedido) |
| `notifications.sent` | `NotificationSent` | notification-service | audit (somente rastreio) | `orderId` |
| `event-audit-log` | `AuditRecord` | todos (interceptor/publisher) | audit-service (`audit-collector`) | `orderId` |

Consumer groups internos (não pausam com o chaos): `<service>-dlq-store`, que materializa a `.DLT` do próprio serviço na tabela `dlq_messages`.

## Headers obrigatórios

Metadados **nunca** vão no payload. Todo registro produzido por `EventPublisher` carrega:

| Header | Conteúdo |
|---|---|
| `eventId` | UUID v7 do evento (ordenável por tempo) |
| `eventType` | `OrderCreated`, `PaymentApproved`, ... |
| `schemaVersion` | inteiro; começa em `1` |
| `correlationId` | id da cadeia inteira; nasce no `X-Correlation-Id` da requisição HTTP |
| `causationId` | `eventId` do evento que causou este; `null` no `OrderCreated` original |
| `producer` | nome do serviço que publicou |
| `producedAt` | ISO-8601 com offset, relógio do produtor |
| `orderId` | redundante com a chave, para leitura sem depender do key deserializer |
| `traceparent` | W3C Trace Context, propagado pelo Micrometer Tracing |
| `x-reprocessed-from` / `x-reprocess-attempt` | só em mensagens republicadas a partir da DLQ |

Consumo com retry acrescenta `kafka_deliveryAttempt` (Spring Kafka). A DLT recebe os headers `kafka_dlt-*` (tópico/partição/offset originais, classe e mensagem da exceção, stack trace) mais `x-dlq-consumer-group` e `x-dlq-attempts`, gravados pelo próprio error handler com o grupo que falhou e o número da última tentativa. No reprocessamento, tudo que começa com `kafka_` ou `x-dlq-` é removido antes de republicar.

## Payload

JSON (UTF-8) do record Java correspondente, sem envelope. Dinheiro é inteiro em centavos + `currency`. Datas em ISO-8601 com offset.

## Evolução de schema sem quebrar consumidores antigos

1. Mudanças **aditivas** (novo campo opcional) não mudam `schemaVersion`; consumidores ignoram campos desconhecidos (`FAIL_ON_UNKNOWN_PROPERTIES=false`).
2. Mudanças **incompatíveis** (renomear, mudar tipo, remover) incrementam `schemaVersion`. O `EventCodec` do consumidor aplica *upcasters* registrados por (`eventType`, versão de origem) até chegar à versão que o código atual entende. Produtores antigos continuam válidos porque a versão viaja no header, não no nome do tópico.
3. Nunca reutilizar um nome de campo com semântica diferente.
4. Quando um consumidor não conhece uma versão **maior** que a sua, a mensagem vai para a DLT com `UnsupportedSchemaVersion`, em vez de ser interpretada errado.

Os testes de contrato (`*ContractTest`) comparam o JSON de cada evento com o schema em `src/test/resources/schemas/<EventType>.v<N>.json`; mudar o payload sem versionar quebra o build.
