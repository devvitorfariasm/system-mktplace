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

## Módulos

| Módulo | Papel |
|---|---|
| `kafka-commons` | Biblioteca compartilhada: tópicos, headers, publisher com auditoria, retry/DLT, idempotência, chaos, DLQ admin, Problem Details |
| `order-service` · `payment-service` · `inventory-service` · `notification-service` · `audit-service` | Serviços de negócio (fases 2 a 6) |
| `gateway` | API Gateway em `:8080` (fase 4) |
| `web` | Front React (fase 5) |

## Rodando

```bash
docker compose up --build          # infra + serviços (serviços entram a partir da fase 2)
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
mvn verify        # unitários + integração (Testcontainers exige Docker ativo)
```

## Documentação

- [Contrato de eventos](docs/eventos.md): tópicos, headers, versionamento de schema.
- ADRs: [particionamento](docs/adr/0001-estrategia-de-particionamento.md) · [outbox](docs/adr/0002-outbox-relay-agendado.md) · [captura do audit log](docs/adr/0003-captura-do-audit-log.md) · [SSE](docs/adr/0004-sse-em-vez-de-websocket.md).

Diagrama de arquitetura, roteamento do gateway, trade-offs e "o que faria diferente" serão preenchidos na fase 8.
