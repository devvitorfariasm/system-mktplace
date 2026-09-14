# system-mktplace

Núcleo de processamento de pedidos de um marketplace em que **o Kafka não é caixa preta**: cada evento, tentativa de consumo, retry, DLQ e lag é visível numa linha do tempo por pedido.

> Enunciado completo em [DESAFIO_TECNICO_SENIOR.md](DESAFIO_TECNICO_SENIOR.md). Plano de execução em [PLANO_FASES.md](PLANO_FASES.md).

## Stack

| Camada | Escolha |
|---|---|
| Backend | Java 21, Spring Boot 4.1, Spring Kafka 4.1, Spring Data JPA, Flyway, springdoc 3 |
| Mensageria | Apache Kafka 4.1 (KRaft), Kafbat UI |
| Banco | PostgreSQL 17, um banco por serviço |
| Observabilidade | Micrometer Tracing + OTLP para Jaeger, logs estruturados (ECS), Prometheus |
| Testes | JUnit 5, Mockito, Testcontainers 2 (Kafka + Postgres), JSON Schema |
| Frontend | React 19, TypeScript, Vite, TanStack Router/Query/Table/Form, Zod, Tailwind v4 |

## Estrutura

Uma única aplicação Spring Boot e um único `pom.xml`. Cada serviço é um **profile** (`order`, `payment`, `inventory`, `notification`, `audit`) que sobe como processo independente no compose, com seu próprio banco e consumer group.

| Pacote | Papel |
|---|---|
| `com.mktplace.commons` | Compartilhado: tópicos, headers, publisher com auditoria, retry/DLT, idempotência, chaos, DLQ admin, Problem Details |
| `com.mktplace.order` · `payment` · `inventory` · `notification` · `audit` | Serviços de negócio, cada um ativo só no seu profile (fases 2 a 6) |
| `gateway/` | Nginx em `:8080` roteando por prefixo (fase 4) |
| `web/` | Front React (fase 5) |

## Rodando

```bash
docker compose up --build          # infra + os cinco serviços (uma imagem, um profile por container)
```

| Serviço | Porta | Rotas na fase atual |
|---|---|---|
| order-service | 8081 | `POST/GET /api/v1/customers`, `POST/GET /api/v1/orders`, `GET /api/v1/orders/{id}/timeline` |
| inventory-service | 8082 | `POST/GET /api/v1/products` |
| payment-service | 8083 | só consumidor (pagamentos nascem de `OrderCreated`) |
| notification-service | 8084 | só consumidor |
| audit-service | 8085 | `GET /api/v1/audit/orders/{id}/timeline` |
| todos | `/admin/**` com `X-Admin-Token` | chaos, DLQ, `/admin/consumer` |

Exemplo mínimo (PowerShell ou bash com `curl`):

```bash
curl -s -X POST localhost:8081/api/v1/customers -H "Content-Type: application/json" -d '{"name":"Ana","email":"ana@ex.com"}'
curl -s -X POST localhost:8082/api/v1/products  -H "Content-Type: application/json" -d '{"sku":"SKU-1","name":"Caneca","price":4990,"initialStock":10}'
curl -s -X POST localhost:8081/api/v1/orders -H "Content-Type: application/json" -H "Idempotency-Key: k1"   -d '{"customerId":"<id do cliente>","currency":"BRL","items":[{"sku":"SKU-1","quantity":2,"unitPrice":4990}]}'
curl -s localhost:8081/api/v1/orders/<id>/timeline
```

| Serviço | URL |
|---|---|
| Kafbat UI | http://localhost:8090 |
| Jaeger | http://localhost:16686 |
| Postgres | `localhost:5432` (usuário/senha padrão `mktplace`, bancos `orders`, `payments`, `inventory`, `notifications`, `audit`) |
| Kafka (host) | `localhost:9094` |

Variáveis opcionais em [.env.example](.env.example).

## Testes

```bash
mvn verify        # unitários + integração com Kafka e Postgres reais (Testcontainers exige Docker ativo)
```

## Documentação

- [Contrato de eventos](docs/eventos.md): tópicos, headers, versionamento de schema.
- ADRs: [particionamento](docs/adr/0001-estrategia-de-particionamento.md) · [outbox](docs/adr/0002-outbox-relay-agendado.md) · [captura do audit log](docs/adr/0003-captura-do-audit-log.md) · [SSE](docs/adr/0004-sse-em-vez-de-websocket.md) · [eventos fora de ordem](docs/adr/0005-eventos-fora-de-ordem-no-projetor.md).

Diagrama de arquitetura, roteamento do gateway, trade-offs e "o que faria diferente" serão preenchidos na fase 8.
