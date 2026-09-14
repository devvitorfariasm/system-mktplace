# Desafio Técnico — Engenheiro(a) de Software Sênior

**Stack obrigatória:** Java 21+ (Spring Boot 4.x) · React 18+ · Apache Kafka · PostgreSQL 15+
**Prazo sugerido:** 5 a 7 dias corridos
**Entrega:** repositório Git público ou privado (com acesso liberado ao avaliador)

---

## 1. Contexto

Você foi contratado(a) para construir o núcleo de um sistema de **processamento de pedidos** de um marketplace. O negócio exige que **qualquer pessoa consiga acompanhar, de ponta a ponta, o que aconteceu com um pedido** — não só o status final, mas cada evento que trafegou pelo Kafka, quem produziu, quem consumiu, quando, em qual partição, quantas tentativas foram feitas e por quê algo falhou.

Em outras palavras: **o Kafka não pode ser uma caixa preta.** A observabilidade do fluxo de eventos é o produto, não um detalhe de infraestrutura.

---

## 2. Cenário de negócio

Um pedido passa pelos seguintes estágios, cada um sob responsabilidade de um serviço distinto:

```
[React]  →  order-service  →  payment-service  →  inventory-service  →  notification-service
             (cria pedido)     (aprova/recusa)      (reserva estoque)      (notifica cliente)
```

Regras:

1. O cliente cria um pedido pelo front-end (itens + quantidade + valor).
2. `order-service` persiste o pedido em PostgreSQL com status `CREATED` e publica `OrderCreated`.
3. `payment-service` consome `OrderCreated`, simula o pagamento (aprovação aleatória com ~80% de sucesso, latência de 1–3 s) e publica `PaymentApproved` ou `PaymentRejected`.
4. `inventory-service` consome `PaymentApproved`, tenta reservar estoque (tabela própria no Postgres). Se não houver estoque, publica `StockUnavailable`; senão, `StockReserved`.
5. `notification-service` consome os eventos terminais (`StockReserved`, `PaymentRejected`, `StockUnavailable`) e registra a notificação.
6. `order-service` consome todos os eventos de resultado e atualiza o status do pedido (`PAID`, `PAYMENT_FAILED`, `CONFIRMED`, `OUT_OF_STOCK`).

 implementar os quatro serviços em uma única aplicação Spring Boot com profiles distintos — desde que cada um tenha seu **próprio consumer group** e rode como **processo independente**.

---

## 3. O requisito central: rastreabilidade ponta a ponta do Kafka

Este é o coração do desafio. O front-end deve ter uma tela **"Linha do tempo do pedido"** onde, ao selecionar um pedido, o usuário vê **todos os eventos Kafka relacionados a ele**, em ordem cronológica, com os seguintes dados para cada evento:

| Campo | Descrição |
|---|---|
| `eventType` | Nome do evento (`OrderCreated`, `PaymentApproved`, …) |
| `topic` | Tópico onde foi publicado |
| `partition` / `offset` | Posição exata no Kafka |
| `key` | Chave da mensagem (deve ser o `orderId`) |
| `producer` | Serviço que publicou |
| `producedAt` | Timestamp de publicação (do producer) |
| `consumers[]` | Lista de consumers que processaram, cada um com: `service`, `consumerGroup`, `consumedAt`, `processingTimeMs`, `attempt`, `status` (`SUCCESS` / `RETRY` / `DLQ`), `errorMessage` |
| `correlationId` | ID que amarra toda a cadeia originada pelo mesmo pedido |
| `causationId` | ID do evento que causou este evento |
| `headers` | Headers Kafka relevantes (ex.: `traceparent`, `schemaVersion`) |

### Como capturar isso

Você decide a arquitetura, mas o avaliador espera algo nessa direção:

- Um **tópico de auditoria** (ex.: `event-audit-log`) para o qual **todo producer e todo consumer** publica um registro de "o que eu fiz com esse evento". Interceptors do Spring Kafka (`ProducerInterceptor` / `RecordInterceptor`) ou um `Aspect` são caminhos naturais.
- Um **consumer de auditoria** que materializa esses registros em uma tabela PostgreSQL desnormalizada, pronta para consulta pelo front.
- Propagação obrigatória de `correlationId` e `causationId` via **headers Kafka**, nunca no payload.

### Visualização em tempo real

A tela da linha do tempo deve **atualizar sem refresh** conforme os eventos vão acontecendo (SSE ou WebSocket). O usuário cria o pedido e assiste, evento a evento, o fluxo se completar — incluindo os retries e as falhas.

### Painel de saúde dos consumers

Uma segunda tela, **"Consumers"**, deve exibir por consumer group:

- Tópicos assinados e partições atribuídas
- **Lag atual por partição** (obtido via `AdminClient` do Kafka, não via estimativa)
- Throughput dos últimos 60 s (mensagens/s)
- Quantidade de mensagens em DLQ

---

## 4. Requisitos funcionais do front-end (React)

| Tela | O que precisa ter |
|---|---|
| **Novo pedido** | Formulário com itens (nome, quantidade, preço). Após criar, redireciona para a linha do tempo. |
| **Lista de pedidos** | Tabela com `orderId`, status, valor, data, e um indicador visual de "em andamento / finalizado". Filtro por status. |
| **Linha do tempo do pedido** | Descrito na seção 3. Deve funcionar como uma visualização de **grafo/cadeia** (evento → causou → evento), não apenas uma lista. |
| **Consumers** | Descrito na seção 3. |
| **DLQ** | Lista das mensagens em dead-letter, com payload, motivo do erro, e um botão **"Reprocessar"** que republica a mensagem no tópico original preservando os headers originais. |

Requisitos técnicos do front:

- TypeScript obrigatório.
- Estado de servidor com React Query (TanStack) ou equivalente; **sem** Redux para cache de API.
- Tratamento de loading, erro e estado vazio em todas as telas.
- Nenhuma lib de UI é exigida; se usar, justifique no README.

---

## 5. Requisitos técnicos do back-end (Java)

### Kafka

- Tópicos criados via código (`NewTopic` beans) ou script versionado — **não** criação automática.
- `orderId` como chave de partição em **todos** os tópicos de negócio. Explique no seu README por quê.
- Mínimo de **3 partições** por tópico de negócio.
- **Retry com backoff exponencial** (ex.: 3 tentativas) seguido de **Dead Letter Topic** por tópico (`<topic>.DLT`).
- **Idempotência no consumer**: reprocessar a mesma mensagem duas vezes **não** pode gerar efeito duplicado. Demonstre isso com um teste.
- Producer configurado com `acks=all` e `enable.idempotence=true`.
- Serialização em JSON com **versionamento de schema** via header (`schemaVersion`). Não é obrigatório Schema Registry, mas explique como você evoluiria um evento sem quebrar consumers antigos.

### Consistência entre PostgreSQL e Kafka

O `order-service` **não pode** salvar no banco e depois publicar no Kafka em duas operações desconectadas. Implemente o **Transactional Outbox Pattern** (tabela `outbox` + relay que publica e marca como enviado). Debezium é bem-vindo, mas um relay agendado também é aceito.

### PostgreSQL

- Migrations versionadas (Flyway ou Liquibase).
- Índices justificados. Espera-se pelo menos índice em `(order_id, produced_at)` na tabela de auditoria.
- A consulta da linha do tempo de um pedido deve rodar em **uma única query** (ou uma por agregação) — nada de N+1.

### Injeção de falhas (obrigatório)

Para que o avaliador **veja** o Kafka se comportando sob estresse, exponha endpoints de controle no `payment-service` e `inventory-service`:

```
POST /admin/chaos/fail-next?count=N      # as próximas N mensagens lançam exceção
POST /admin/chaos/slow?ms=N              # adiciona N ms de latência no processamento
POST /admin/chaos/pause                  # pausa o consumer (container.pause())
POST /admin/chaos/resume
```

O efeito de cada um deve ser visível na linha do tempo e no painel de consumers (retries aparecendo, lag subindo, DLQ enchendo).

---

## 6. Contrato de API (REST)

Todas as rotas ficam sob o prefixo `/api/v1`. Cada serviço expõe apenas as rotas do seu domínio; o front conversa com um **API Gateway** (Spring Cloud Gateway ou Nginx) em `http://localhost:8080` que roteia por prefixo.

### 6.1 Convenções gerais (obrigatórias)

| Item | Regra |
|---|---|
| Identificadores | UUID v7 (ordenável por tempo). Nunca expor IDs sequenciais do banco. |
| Paginação | Cursor-based: `?limit=20&cursor=<opaque>`. Resposta traz `nextCursor` (null no fim). Offset-based **não** é aceito nas listagens de eventos. |
| Filtros | Query params simples (`status=PAID`), ranges com sufixo (`createdAt_gte`, `createdAt_lte`), múltiplos valores com vírgula (`status=PAID,CONFIRMED`). |
| Ordenação | `?sort=createdAt:desc,total:asc` |
| Idempotência em POST | Header `Idempotency-Key` obrigatório em `POST /orders`. Mesma key + mesmo body → retorna a resposta original com `200` e header `Idempotent-Replayed: true`. Mesma key + body diferente → `422`. |
| Concorrência otimista | `PUT`/`PATCH` exigem header `If-Match: "<version>"`. Versão divergente → `412 Precondition Failed`. Toda resposta de recurso traz `ETag`. |
| Correlação | Cliente pode enviar `X-Correlation-Id`; se ausente, o gateway gera. Toda resposta ecoa o header, e ele vira o `correlationId` dos eventos Kafka da cadeia. |
| Erros | RFC 9457 (Problem Details), `Content-Type: application/problem+json`. |
| Datas | ISO-8601 com offset (`2026-09-14T13:45:00-03:00`). |
| Dinheiro | Inteiro em centavos + `currency` (`"BRL"`). Nunca `double`. |
| Rate limit | `429` com `Retry-After` nos endpoints de escrita (limite a seu critério, documentado). |

Formato de erro:

```json
{
  "type": "https://api.example.com/problems/insufficient-stock",
  "title": "Estoque insuficiente",
  "status": 409,
  "detail": "Produto SKU-123 possui 2 unidades; solicitado 5",
  "instance": "/api/v1/orders/0192a3b4-...",
  "correlationId": "c9f1...",
  "errors": [ { "field": "items[0].quantity", "message": "excede disponível" } ]
}
```

Formato de listagem:

```json
{
  "data": [ ... ],
  "page": { "limit": 20, "nextCursor": "eyJpZCI6...", "hasMore": true },
  "meta": { "totalEstimate": 1342 }
}
```

---

### 6.2 `order-service` — Pedidos

| Método | Rota | Descrição | Sucesso | Erros |
|---|---|---|---|---|
| `POST` | `/orders` | Cria pedido. Grava no Postgres + outbox na **mesma transação**. | `201` + `Location` | `400`, `422` (item duplicado, qtd ≤ 0, cliente inexistente), `429` |
| `GET` | `/orders` | Lista paginada. Filtros: `status`, `customerId`, `createdAt_gte/lte`, `total_gte/lte`, `q` (busca em itens). | `200` | `400` (cursor inválido) |
| `GET` | `/orders/{orderId}` | Detalhe com itens, status atual, `version`, `statusHistory[]`. | `200` + `ETag` | `404` |
| `PATCH` | `/orders/{orderId}` | Edita **apenas** `shippingAddress` e `notes`, e **apenas** enquanto `CREATED`. | `200` | `404`, `409` (status não permite), `412` |
| `POST` | `/orders/{orderId}/cancel` | Cancela. Permitido em `CREATED` e `PAID`; se `PAID`, publica `OrderCancelled` que dispara estorno e liberação de estoque. | `202` | `404`, `409` (já `CONFIRMED`/`CANCELLED`) |
| `POST` | `/orders/{orderId}/retry` | Reinicia o fluxo de um pedido em `PAYMENT_FAILED` ou `OUT_OF_STOCK` — republica `OrderCreated` com **novo `causationId`** e mesmo `correlationId`. | `202` | `404`, `409` |
| `DELETE` | `/orders/{orderId}` | Soft delete (`deletedAt`). Só em `CANCELLED`. Some das listagens, mas a linha do tempo continua acessível. | `204` | `404`, `409` |
| `GET` | `/orders/{orderId}/timeline` | **Rota central do desafio.** Retorna a cadeia de eventos (ver 6.7). | `200` | `404` |
| `GET` | `/orders/{orderId}/timeline/stream` | SSE. Emite `event: kafka-event` a cada novo registro de auditoria do pedido; envia `event: done` quando o pedido chega a status terminal. Suporta `Last-Event-ID` para reconexão sem perda. | `200 text/event-stream` | `404` |
| `GET` | `/orders/stream` | SSE global: mudanças de status de qualquer pedido (alimenta a lista sem polling). | `200` | — |
| `GET` | `/orders/stats` | Agregados: contagem por status, ticket médio, p50/p95 de tempo `CREATED → CONFIRMED` nas últimas 24 h. Uma única query com `FILTER (WHERE ...)`. | `200` | — |

`POST /orders` — request:

```json
{
  "customerId": "0192a3b4-1111-7000-8000-000000000001",
  "items": [
    { "sku": "SKU-123", "quantity": 2, "unitPrice": 4990 },
    { "sku": "SKU-456", "quantity": 1, "unitPrice": 12000 }
  ],
  "currency": "BRL",
  "shippingAddress": { "street": "...", "city": "...", "zip": "..." },
  "notes": "Entregar na portaria"
}
```

Response `201`:

```json
{
  "id": "0192a3b4-2222-7000-8000-00000000abcd",
  "status": "CREATED",
  "total": 21980,
  "currency": "BRL",
  "version": 0,
  "correlationId": "c9f1...",
  "createdAt": "2026-09-14T13:45:00-03:00",
  "_links": {
    "self": "/api/v1/orders/0192a3b4-...",
    "timeline": "/api/v1/orders/0192a3b4-.../timeline",
    "stream": "/api/v1/orders/0192a3b4-.../timeline/stream"
  }
}
```

**Máquina de estados** (qualquer transição fora dela → `409`):

```
CREATED ──► PAID ──► CONFIRMED
   │          │
   │          └──► OUT_OF_STOCK ──► (retry) ──► CREATED
   │
   ├──► PAYMENT_FAILED ──► (retry) ──► CREATED
   │
   └──► CANCELLED  ◄── PAID (via cancel, com estorno)
```

---

### 6.3 `order-service` — Clientes

| Método | Rota | Descrição | Sucesso | Erros |
|---|---|---|---|---|
| `POST` | `/customers` | Cria cliente. `email` único (índice `UNIQUE`, case-insensitive via `citext` ou `lower()`). | `201` | `400`, `409` (email já existe) |
| `GET` | `/customers` | Lista paginada. Filtros: `q` (nome/email, `ILIKE` com índice `pg_trgm`), `createdAt_gte`. | `200` | — |
| `GET` | `/customers/{customerId}` | Detalhe + `ordersSummary` (`{ total, byStatus }`). | `200` + `ETag` | `404` |
| `PUT` | `/customers/{customerId}` | Substituição completa. | `200` | `404`, `409`, `412` |
| `DELETE` | `/customers/{customerId}` | Só se não houver pedidos em andamento (`CREATED`/`PAID`). Anonimiza dados pessoais (LGPD) em vez de apagar a linha. | `204` | `404`, `409` |
| `GET` | `/customers/{customerId}/orders` | Atalho para `GET /orders?customerId=`. | `200` | `404` |

---

### 6.4 `inventory-service` — Produtos e estoque

| Método | Rota | Descrição | Sucesso | Erros |
|---|---|---|---|---|
| `POST` | `/products` | Cria produto (`sku` único, `name`, `price`, `initialStock`). | `201` | `400`, `409` |
| `GET` | `/products` | Lista. Filtros: `q`, `stock_lte` (para "abaixo do mínimo"), `active`. | `200` | — |
| `GET` | `/products/{sku}` | Detalhe com `available`, `reserved`, `total`. `available = total - reserved`, calculado no banco, nunca em Java. | `200` + `ETag` | `404` |
| `PATCH` | `/products/{sku}` | Edita `name`, `price`, `active`. Preço muda **não** afeta pedidos já criados. | `200` | `404`, `412` |
| `DELETE` | `/products/{sku}` | Desativa (`active=false`). Impede novos pedidos; reservas existentes continuam válidas. | `204` | `404` |
| `POST` | `/products/{sku}/stock/adjust` | Ajuste manual `{ "delta": +50, "reason": "..." }`. Publica `StockAdjusted`. Gera linha em `stock_movements`. | `200` | `404`, `409` (resultaria em negativo) |
| `GET` | `/products/{sku}/stock/movements` | Histórico: reservas, liberações, ajustes — cada linha com `orderId` (quando houver), `delta`, `reason`, `causedByEventId`. | `200` | `404` |
| `GET` | `/reservations` | Lista reservas ativas. Filtros: `orderId`, `sku`, `status` (`ACTIVE`/`RELEASED`/`CONSUMED`). | `200` | — |
| `POST` | `/reservations/{reservationId}/release` | Libera manualmente (uso operacional). Publica `StockReleased`. | `202` | `404`, `409` |

A reserva deve usar `SELECT ... FOR UPDATE` (ou `UPDATE ... WHERE available >= ? RETURNING`) para evitar overselling sob concorrência. Teste isso com pelo menos 2 consumers concorrentes no mesmo SKU.

---

### 6.5 `payment-service` — Pagamentos

| Método | Rota | Descrição | Sucesso | Erros |
|---|---|---|---|---|
| `GET` | `/payments` | Lista. Filtros: `orderId`, `status` (`APPROVED`/`REJECTED`/`REFUNDED`), `processedAt_gte`. | `200` | — |
| `GET` | `/payments/{paymentId}` | Detalhe com `attempts[]` (cada tentativa com `attempt`, `result`, `latencyMs`, `reason`). | `200` | `404` |
| `POST` | `/payments/{paymentId}/refund` | Estorno manual `{ "reason": "..." }`. Só em `APPROVED`. Publica `PaymentRefunded`. | `202` | `404`, `409` |
| `GET` | `/payments/stats` | Taxa de aprovação, p95 de latência, contagem por motivo de recusa (última 1 h / 24 h). | `200` | — |

Não existe `POST /payments` público: pagamentos **só** nascem via consumo de `OrderCreated`. Documente por quê.

---

### 6.6 `notification-service` — Notificações

| Método | Rota | Descrição | Sucesso | Erros |
|---|---|---|---|---|
| `GET` | `/notifications` | Lista. Filtros: `orderId`, `customerId`, `channel` (`EMAIL`/`SMS`/`PUSH`), `status` (`SENT`/`FAILED`/`PENDING`). | `200` | — |
| `GET` | `/notifications/{notificationId}` | Detalhe com template renderizado e `triggeredByEventId`. | `200` | `404` |
| `POST` | `/notifications/{notificationId}/resend` | Reenvia. | `202` | `404` |
| `GET` | `/notifications/templates` | Lista templates por `eventType`. | `200` | — |
| `PUT` | `/notifications/templates/{eventType}` | Atualiza template (`subject`, `body` com placeholders `{{orderId}}`, `{{total}}`). Validar placeholders desconhecidos → `422`. | `200` | `404`, `422` |

---

### 6.7 `audit-service` — Rastreabilidade Kafka (o coração do desafio)

Pode viver dentro do `order-service` ou como quinto serviço; o avaliador prefere separado.

| Método | Rota | Descrição | Sucesso | Erros |
|---|---|---|---|---|
| `GET` | `/audit/events` | Busca global de eventos. Filtros: `correlationId`, `causationId`, `orderId`, `topic`, `eventType`, `producer`, `consumerGroup`, `status` (`SUCCESS`/`RETRY`/`DLQ`), `partition`, `offset_gte/lte`, `producedAt_gte/lte`. Cursor **obrigatoriamente** baseado em `(producedAt, id)`. | `200` | `400` |
| `GET` | `/audit/events/{eventId}` | Detalhe completo: headers, payload bruto (base64 + JSON parseado), lista de consumers com cada tentativa. | `200` | `404` |
| `GET` | `/audit/events/{eventId}/lineage` | Ancestrais e descendentes via `causationId` — resposta em forma de árvore. Implementar com **CTE recursiva** no Postgres. | `200` | `404` |
| `GET` | `/audit/correlations/{correlationId}` | Tudo que aconteceu numa cadeia, agrupado por serviço, com tempo total ponta a ponta e o "caminho crítico" (soma dos maiores `processingTimeMs` por hop). | `200` | `404` |
| `GET` | `/audit/orders/{orderId}/timeline` | Mesma resposta de `GET /orders/{orderId}/timeline` (o `order-service` faz proxy para cá). | `200` | `404` |
| `GET` | `/audit/topics` | Lista tópicos com partições, retenção, `cleanup.policy`, contagem de mensagens (via `AdminClient.listOffsets`), e quantos registros de auditoria existem para cada um. | `200` | — |
| `GET` | `/audit/topics/{topic}/partitions/{partition}/messages` | Leitura direta do Kafka (não da tabela): `?offset=N&limit=20` — cria um consumer efêmero, faz `seek`, lê e fecha. Mostra ao usuário a mensagem "como está no broker". | `200` | `404`, `416` (offset fora do range) |
| `GET` | `/audit/consumers` | Consumer groups com: estado, membros, partições atribuídas, **lag por partição** (`AdminClient.listConsumerGroupOffsets` vs `listOffsets`), throughput 60 s, último rebalance. | `200` | — |
| `GET` | `/audit/consumers/{groupId}` | Detalhe de um grupo. | `200` | `404` |
| `GET` | `/audit/consumers/{groupId}/lag/history` | Série temporal de lag (amostrada a cada 5 s pelo próprio serviço, guardada no Postgres com retenção de 24 h). `?window=1h`. | `200` | `404` |
| `POST` | `/audit/consumers/{groupId}/reset-offsets` | `{ "topic": "...", "to": "earliest" \| "latest" \| { "timestamp": "..." } \| { "offsets": { "0": 120 } } }`. Exige grupo inativo (`409` caso contrário). Fica atrás de `ROLE_ADMIN`. | `202` | `404`, `409`, `403` |
| `GET` | `/audit/consumers/stream` | SSE com snapshot de lag de todos os grupos a cada 2 s. | `200` | — |
| `GET` | `/audit/stats/latency` | Percentis de latência por hop (`producedAt` → `consumedAt`) por par `(topic, consumerGroup)`. `?window=1h&percentiles=50,95,99`. | `200` | — |
| `GET` | `/audit/search` | Busca em texto livre no payload (`jsonb` + índice GIN): `?q=SKU-123` retorna eventos cujo payload contém o termo. | `200` | `400` |

Response de `/orders/{orderId}/timeline` (referência):

```json
{
  "orderId": "0192a3b4-...",
  "correlationId": "c9f1...",
  "currentStatus": "CONFIRMED",
  "startedAt": "2026-09-14T13:45:00.120-03:00",
  "finishedAt": "2026-09-14T13:45:04.870-03:00",
  "totalDurationMs": 4750,
  "criticalPathMs": 4210,
  "events": [
    {
      "eventId": "e-0001",
      "eventType": "OrderCreated",
      "schemaVersion": 1,
      "topic": "orders.created",
      "partition": 2,
      "offset": 8841,
      "key": "0192a3b4-...",
      "producer": "order-service",
      "producedAt": "2026-09-14T13:45:00.120-03:00",
      "causationId": null,
      "headers": { "traceparent": "00-...", "schemaVersion": "1" },
      "consumers": [
        {
          "service": "payment-service",
          "consumerGroup": "payment-service-v1",
          "attempts": [
            { "attempt": 1, "consumedAt": "...", "processingTimeMs": 1830, "status": "RETRY", "errorMessage": "Gateway timeout" },
            { "attempt": 2, "consumedAt": "...", "processingTimeMs": 1204, "status": "SUCCESS", "errorMessage": null }
          ],
          "emitted": ["e-0002"]
        },
        {
          "service": "order-service",
          "consumerGroup": "order-status-projector",
          "attempts": [ { "attempt": 1, "consumedAt": "...", "processingTimeMs": 12, "status": "SUCCESS" } ],
          "emitted": []
        }
      ]
    },
    {
      "eventId": "e-0002",
      "eventType": "PaymentApproved",
      "causationId": "e-0001",
      "...": "..."
    }
  ],
  "dlq": [],
  "gaps": [
    { "between": ["e-0001", "e-0002"], "waitingMs": 3034, "note": "payment-service retry #1 falhou" }
  ]
}
```

---

### 6.8 Rotas de operação e chaos (todos os serviços consumidores)

Prefixo `/admin`, protegidas por header `X-Admin-Token` (valor via env). Sem token → `401`; token errado → `403`.

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/admin/chaos/fail-next?count=N&exception=TRANSIENT\|POISON` | `TRANSIENT` → passa pelo retry; `POISON` → vai direto para DLQ (ex.: `DeserializationException`). |
| `POST` | `/admin/chaos/slow?ms=N&count=M` | Latência artificial nas próximas M mensagens. |
| `POST` | `/admin/chaos/pause` / `resume` | `container.pause()` / `resume()` no listener. |
| `POST` | `/admin/chaos/crash` | `System.exit(1)` após 500 ms — para observar rebalance e o `docker compose` reiniciar o container. |
| `GET` | `/admin/chaos/status` | Estado atual de todas as injeções ativas. |
| `DELETE` | `/admin/chaos` | Limpa todas as injeções. |
| `GET` | `/admin/dlq` | Mensagens na DLT deste serviço: `eventId`, `originalTopic`, `originalPartition/Offset`, `exceptionClass`, `stackTrace` (truncado), `attempts`, `failedAt`, payload. |
| `GET` | `/admin/dlq/{messageId}` | Detalhe completo com stack trace inteiro. |
| `POST` | `/admin/dlq/{messageId}/reprocess` | Republica no tópico original **preservando headers originais** + adiciona `x-reprocessed-from` e `x-reprocess-attempt`. |
| `POST` | `/admin/dlq/reprocess-all?originalTopic=...` | Reprocessamento em lote com `202` + `jobId`; progresso em `GET /admin/jobs/{jobId}`. |
| `DELETE` | `/admin/dlq/{messageId}` | Descarta definitivamente (registra evento `DlqMessageDiscarded` na auditoria com `reason` obrigatório no body). |
| `GET` | `/admin/consumer` | Estado do listener local: `RUNNING`/`PAUSED`, partições atribuídas, offsets commitados, `inFlight`. |

---

### 6.9 Gateway e infraestrutura

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/health` | Agrega `actuator/health` de todos os serviços + `kafka: UP/DOWN` + `postgres: UP/DOWN`. |
| `GET` | `/openapi.json` | OpenAPI 3.1 **agregado** de todos os serviços (springdoc). |
| `GET` | `/docs` | Swagger UI. |
| `GET` | `/actuator/prometheus` | Por serviço, na porta de management. |

Roteamento do gateway (documente no README):

```
/api/v1/orders/**, /api/v1/customers/**   → order-service:8081
/api/v1/products/**, /api/v1/reservations/** → inventory-service:8082
/api/v1/payments/**                        → payment-service:8083
/api/v1/notifications/**                   → notification-service:8084
/api/v1/audit/**                           → audit-service:8085
/api/v1/admin/{service}/**                 → {service}:80xx/admin/**   (rewrite)
```

### 6.10 O que o avaliador vai testar via API, sem abrir o front

1. `POST /orders` duas vezes com o mesmo `Idempotency-Key` → segunda responde `200` + `Idempotent-Replayed: true`, **um único** `OrderCreated` no Kafka.
2. `PATCH /orders/{id}` com `If-Match` desatualizado → `412`.
3. `POST /admin/payment-service/chaos/fail-next?count=3&exception=TRANSIENT`, criar pedido, `GET /audit/orders/{id}/timeline` → 3 attempts `RETRY` + 1 `SUCCESS` no mesmo evento.
4. `POST /admin/inventory-service/chaos/pause`, criar 10 pedidos, `GET /audit/consumers` → lag ≥ 10 no grupo do inventory; `resume` → lag volta a 0 e SSE de consumers reflete isso.
5. `POISON` em 1 mensagem → aparece em `GET /admin/inventory-service/dlq`; `reprocess` → some da DLQ e a timeline mostra `x-reprocessed-from` no header do novo registro.
6. Dois `POST /orders` concorrentes para um SKU com estoque 1 → exatamente um `CONFIRMED` e um `OUT_OF_STOCK`; `GET /products/{sku}` → `available: 0`, `reserved: 1`.
7. `GET /audit/events/{id}/lineage` de um `StockReserved` → árvore `OrderCreated → PaymentApproved → StockReserved`.
8. `GET /audit/topics/orders.created/partitions/2/messages?offset=8841` → payload idêntico ao que está em `GET /audit/events/{eventId}`.

---

## 7. Observabilidade adicional

- **Tracing distribuído** com OpenTelemetry (ou Micrometer Tracing): o `traceId` deve ser o mesmo desde a requisição HTTP do React até o último consumer. Suba um Jaeger ou Zipkin no `docker-compose`.
- **Logs estruturados** (JSON) contendo `traceId`, `correlationId`, `orderId`, `topic`, `partition`, `offset` em toda linha relacionada a Kafka.
- Métricas expostas via Actuator/Prometheus (bônus: dashboard Grafana pronto).

---

## 8. Infraestrutura e execução

Tudo deve subir com **um único comando**:

```bash
docker compose up --build
```

Serviços esperados no compose:

- Kafka (KRaft, sem Zookeeper) + **Kafka UI** (Kafbat/Redpanda Console/AKHQ)
- PostgreSQL
- Os 4 serviços Java
- O front React (servido via Nginx ou Vite preview)
- Jaeger/Zipkin
- (opcional) Prometheus + Grafana

Um script `./scripts/demo.sh` deve:

1. Criar 20 pedidos em sequência
2. Ativar `fail-next?count=5` no `payment-service`
3. Criar mais 10 pedidos
4. Pausar o `inventory-service` por 15 s e retomar

Ao final, o avaliador deve conseguir abrir o front e **ver na linha do tempo os retries, a DLQ, o lag subindo e voltando**.

---

## 9. Testes

Esperamos, no mínimo:

| Tipo | Ferramenta sugerida | Cobrir |
|---|---|---|
| Unitário | JUnit 5 + Mockito | Regras de negócio, mapeamentos |
| Integração Kafka | **Testcontainers** (Kafka + Postgres) | Fluxo completo `OrderCreated → CONFIRMED`; retry → DLQ; idempotência; outbox relay |
| Contrato de eventos | JSON Schema ou AssertJ | Payload de cada evento não muda sem versionar |
| Front | Vitest + Testing Library | Linha do tempo renderiza eventos corretamente a partir de um mock de SSE |

**Não** aceitaremos `@EmbeddedKafka` como substituto do Testcontainers para os testes de integração.

---

## 10. Documentação exigida no seu README

1. **Diagrama de arquitetura** (Mermaid, draw.io, o que preferir) mostrando serviços, tópicos e o fluxo de auditoria.
2. **Decisões técnicas** — no formato ADR curto (Contexto / Decisão / Consequências) para pelo menos:
   - Estratégia de particionamento
   - Outbox: relay agendado vs. CDC
   - Como o audit log é capturado (interceptor, aspect, explícito)
   - Como o front recebe updates (SSE vs. WebSocket)
3. **O que você faria diferente com mais tempo.**
4. **Trade-offs conhecidos** — o que está simplificado e por quê.
5. Como rodar, como testar, como executar o cenário de demo.

---

## 11. Critérios de avaliação

| Critério | Peso | O que observamos |
|---|---|---|
| **Rastreabilidade ponta a ponta** | 30% | A linha do tempo mostra tudo? Correlation/causation corretos? Retries e DLQ visíveis? Atualiza em tempo real? |
| **Corretude Kafka** | 20% | Idempotência real, outbox, particionamento, retry/DLQ, configuração de producer |
| **Qualidade de código** | 15% | Coesão, nomes, separação de responsabilidades, ausência de over-engineering |
| **Testes** | 15% | Testcontainers funcionando, cenários de falha cobertos |
| **Front-end** | 10% | UX da linha do tempo, tratamento de estados, código TypeScript limpo |
| **Documentação e decisões** | 10% | ADRs coerentes, trade-offs honestos, demo reproduzível |

### O que desqualifica

- `docker compose up` não sobe ou exige passos manuais não documentados
- Evento perdido ou duplicado no cenário de demo sem que isso apareça na linha do tempo
- Ausência de testes de integração com Kafka real
- Segredos commitados

### O que diferencia um sênior

- Explicar **por que** o lag subiu e como o rebalance afetou as partições, não só mostrar o número
- Tratar `PaymentRejected` após `StockReserved` (evento fora de ordem) de forma consciente
- Compaction, retenção ou tiered storage mencionados no lugar certo — ou justificativa de por que não se aplicam
- Graceful shutdown do consumer sem perda de offset

---

## 12. Entrega

Envie o link do repositório com:

- Código-fonte completo
- `docker-compose.yml` funcional
- `README.md` conforme seção 10
- (opcional) vídeo curto (≤ 5 min) rodando o `demo.sh` e navegando pela linha do tempo

Dúvidas sobre o enunciado podem ser enviadas ao avaliador. Perguntas boas contam a seu favor.

Boa sorte.
