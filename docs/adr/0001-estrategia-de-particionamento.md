# ADR 0001 · Estratégia de particionamento

**Status:** rascunho (fase 0) · revisar na fase 8

## Contexto
O fluxo de um pedido é uma cadeia de eventos (`OrderCreated → PaymentApproved → StockReserved → ...`) consumida por serviços distintos. Cada serviço precisa ver os eventos **do mesmo pedido em ordem**; entre pedidos diferentes a ordem não importa. O desafio exige 3 partições por tópico e `orderId` como chave.

## Decisão
- Um tópico por tipo de evento (`orders.created`, `payments.approved`, ...), 3 partições cada.
- Chave = `orderId` em todo tópico do fluxo do pedido. O particionador padrão (murmur2 da chave) garante que todos os eventos de um pedido caiam na **mesma partição** de um dado tópico, logo são entregues em ordem para o mesmo consumidor do grupo.
- `event-audit-log` também usa `orderId` como chave: os registros de auditoria de um pedido chegam em ordem ao coletor, o que simplifica montar a linha do tempo.
- `inventory.adjusted` (ajuste manual de estoque) usa `sku`: não há pedido e o que precisa ser serializado é o saldo do produto.
- DLT com o **mesmo** número de partições, e o recuperador publica na mesma partição da origem, preservando a informação "onde estava".

## Consequências
- Ordem garantida apenas dentro de um pedido e de um tópico; ordem **entre tópicos** não é garantida (ver tratamento de `PaymentRejected` após `StockReserved` na fase 6).
- Paralelismo máximo = 3 consumidores por grupo. Aumentar partições depois muda o mapeamento chave→partição; documentar como operação de migração.
- Um pedido "quente" não existe (cada pedido gera poucos eventos), então não há partição desbalanceada por chave.
