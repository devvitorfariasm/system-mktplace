# ADR 0004 · Front recebe atualizações por SSE, não WebSocket

**Status:** rascunho (fase 0) · implementação nas fases 3 e 5

## Contexto
A linha do tempo, a lista de pedidos e o painel de consumers devem atualizar sem refresh. O fluxo é **unidirecional** (servidor → navegador); o navegador só faz GET/POST normais.

## Decisão
- `text/event-stream` com `SseEmitter` (Spring MVC + virtual threads). Endpoints: `/orders/{id}/timeline/stream`, `/orders/stream`, `/audit/consumers/stream`.
- Cada evento leva `id:` = `eventId` do registro de auditoria; o cliente reenvia `Last-Event-ID` ao reconectar e o servidor reenvia o que faltou a partir do Postgres. Reconexão é nativa do `EventSource`.
- `event: done` encerra o stream quando o pedido chega a status terminal.

## Consequências
- Passa por HTTP/1.1 e pelo gateway sem protocolo extra; só é preciso desabilitar buffering no proxy.
- Sem canal cliente→servidor, mas nenhum caso de uso precisa dele.
- Limite de ~6 conexões por origem em HTTP/1.1 no navegador: irrelevante para 2 ou 3 streams simultâneos.
- WebSocket seria justificado se houvesse interação bidirecional (ex.: chat operacional), o que não existe.
