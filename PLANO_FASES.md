# Plano de fases

Ordem por risco: primeiro rastreabilidade e corretude Kafka (50% da avaliação), depois largura de CRUD.

| Fase | Entrega | Status |
|---|---|---|
| 0 | Monorepo, POM pai, compose de infra (Kafka KRaft, Kafbat, Postgres, Jaeger), `.env.example`, CI, contrato de eventos, ADRs em rascunho | feita |
| 1 | `kafka-commons`: tópicos, headers, `EventPublisher` com auditoria, retry exponencial + DLT, `RecordInterceptor` de auditoria, idempotência (`processed_events`), chaos, DLQ store + admin, `X-Admin-Token`, Problem Details, UUID v7, correlação HTTP | feita (18 unitários + 9 ITs com Kafka e Postgres reais) |
| 2 | Fatia vertical: os cinco serviços no mínimo para `OrderCreated → CONFIRMED`, outbox + relay, reserva com `UPDATE ... RETURNING`, coletor de auditoria, `GET /audit/orders/{id}/timeline` em uma query | |
| 3 | audit-service completo: lineage (CTE), correlações, gaps, tópicos e mensagens via AdminClient, lag + histórico + SSE, latência por hop, busca GIN, reset de offsets | |
| 4 | Gateway: rotas, rewrite de `/admin/{service}`, `X-Correlation-Id`, rate limit, health e OpenAPI agregados | |
| 5 | Front núcleo: Novo pedido, Lista (SSE), Linha do tempo em grafo, Consumers, DLQ | |
| 6 | Largura da API: clientes, `PATCH` + `If-Match`, cancel/retry/soft delete, stats, produtos e movimentos, refund, templates, evento fora de ordem | |
| 7 | Observabilidade e infra: trace ponta a ponta, logs JSON, graceful shutdown, Dockerfiles, compose completo, `scripts/demo.sh` | |
| 8 | Hardening: os 8 cenários da seção 6.10 automatizados, contratos JSON Schema, README final com diagrama e ADRs revisados | |

## Decisão em aberto

O enunciado local foi editado para "implementar os quatro serviços em uma única aplicação Spring Boot com profiles distintos". O plano assume **um módulo Maven por serviço** (o avaliador prefere o audit-service separado). Se a intenção for uma única aplicação com profiles, a fase 2 muda: um módulo `app` com `@Profile` por serviço, mantendo `kafka-commons` intacto.
