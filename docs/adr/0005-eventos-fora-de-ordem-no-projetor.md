# ADR 0005 · Eventos fora de ordem no projetor de status

**Status:** aceito (fase 2)

## Contexto
O `order-service` projeta o status consumindo quatro tópicos com um único consumer group. O Kafka só garante ordem dentro de uma partição; entre tópicos não há garantia. Na prática, `StockReserved` chega antes de `PaymentApproved` sempre que a partição de `payments.approved` está presa em retries (chaos, gateway lento), e o teste de fluxo reproduziu isso na primeira execução.

## Decisão
- `StockReserved` e `StockUnavailable` pressupõem causalmente `PaymentApproved` (o `causationId` prova). Se chegam com o pedido ainda em `CREATED`, o projetor aplica a transição implícita `CREATED → PAID` (histórico marcado como "implícito") e depois o evento real.
- O `PaymentApproved` atrasado, ao chegar, não muda nada: fica no `statusHistory` como `applied=false` com o motivo, e portanto aparece na linha do tempo.
- Um evento contraditório (ex.: `PaymentRejected` depois de `StockReserved`) nunca sobrescreve um estado terminal: também é registrado como não aplicado. Estorno e cancelamento são fluxos explícitos (fase 6), não inferidos de eventos atrasados.
- Pedido desconhecido pelo projetor é não-retryable (`PoisonMessageException`): vai direto para a DLT em vez de bloquear a partição por 3 retries.

## Consequências
- O status converge para o mesmo resultado independentemente da ordem de chegada.
- O histórico fica "honesto": mostra o que chegou, quando e o que foi ignorado.
- Alternativa descartada: enfileirar o evento até o pré-requisito chegar. Exigiria estado extra e um timer, sem ganho de correção.
