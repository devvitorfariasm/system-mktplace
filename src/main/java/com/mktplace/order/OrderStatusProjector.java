package com.mktplace.order;

import com.mktplace.commons.consumer.IdempotencyGuard;
import com.mktplace.commons.consumer.PoisonMessageException;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.EventTypes;
import com.mktplace.commons.events.HeaderCodec;
import com.mktplace.commons.events.Topics;
import com.mktplace.commons.util.UuidV7;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Consome os eventos de resultado e projeta o status do pedido pela máquina de estados.
 * Ordem entre tópicos não é garantida: StockReserved/StockUnavailable podem chegar antes de PaymentApproved
 * (a partição de pagamentos pode estar presa em retries). Como esses eventos pressupõem causalmente o
 * pagamento aprovado, o projetor avança o estado implícito (CREATED → PAID, marcado como implícito) e aplica
 * o evento. O PaymentApproved atrasado fica no histórico como "não aplicado", visível na linha do tempo.
 * Um evento realmente contraditório (ex.: PaymentRejected após StockReserved) também não muda o status:
 * só o histórico registra, e a fase 6 trata estorno/cancelamento explicitamente.
 */
@Component
@Profile("order")
public class OrderStatusProjector {

    public static final String GROUP = "order-status-projector";
    private static final Logger log = LoggerFactory.getLogger(OrderStatusProjector.class);
    private static final Map<String, OrderStatus> TARGET = Map.of(
            EventTypes.PAYMENT_APPROVED, OrderStatus.PAID,
            EventTypes.PAYMENT_REJECTED, OrderStatus.PAYMENT_FAILED,
            EventTypes.STOCK_RESERVED, OrderStatus.CONFIRMED,
            EventTypes.STOCK_UNAVAILABLE, OrderStatus.OUT_OF_STOCK);

    private static final Set<OrderStatus> IMPLIES_PAID = Set.of(OrderStatus.CONFIRMED, OrderStatus.OUT_OF_STOCK);

    private final OrderRepository orders;
    private final OrderStatusHistoryRepository history;
    private final IdempotencyGuard guard;
    private final Clock clock;

    public OrderStatusProjector(OrderRepository orders, OrderStatusHistoryRepository history, IdempotencyGuard guard, Clock clock) {
        this.orders = orders;
        this.history = history;
        this.guard = guard;
        this.clock = clock;
    }

    @KafkaListener(id = "order-status-projector", groupId = GROUP, topics = {
            Topics.PAYMENTS_APPROVED, Topics.PAYMENTS_REJECTED, Topics.INVENTORY_RESERVED, Topics.INVENTORY_UNAVAILABLE})
    public void onResultEvent(ConsumerRecord<String, String> record, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        String eventType = HeaderCodec.require(record.headers(), EventHeaders.EVENT_TYPE);
        String eventId = HeaderCodec.require(record.headers(), EventHeaders.EVENT_ID);
        UUID orderId = UUID.fromString(HeaderCodec.require(record.headers(), EventHeaders.ORDER_ID));
        OrderStatus target = TARGET.get(eventType);
        if (target == null) {
            log.warn("order.projector.ignored eventType={} orderId={}", eventType, orderId);
            return;
        }
        guard.runOnce(record, groupId, () -> apply(orderId, target, eventId, eventType));
    }

    private void apply(UUID orderId, OrderStatus target, String eventId, String eventType) {
        // pedido desconhecido não melhora com retry: vai direto para a DLQ, sem prender a partição
        Order order = orders.findByIdForUpdate(orderId)
                .orElseThrow(() -> new PoisonMessageException("Pedido " + orderId + " não existe neste order-service"));
        OffsetDateTime now = OffsetDateTime.now(clock);
        if (order.getStatus() == OrderStatus.CREATED && IMPLIES_PAID.contains(target)) {
            order.tryTransitionTo(OrderStatus.PAID, now);
            history.save(new OrderStatusHistory(UuidV7.generate(), orderId, OrderStatus.CREATED, OrderStatus.PAID, eventId,
                    eventType, true, "Implícito: " + eventType + " pressupõe PaymentApproved, que ainda não chegou", now));
            log.warn("order.status.implied orderId={} CREATED -> PAID by={}", orderId, eventType);
        }
        OrderStatus from = order.getStatus();
        boolean applied = order.tryTransitionTo(target, now);
        String reason = applied ? null
                : "Fora de ordem: %s chegou com o pedido em %s e não foi aplicado".formatted(eventType, from);
        history.save(new OrderStatusHistory(UuidV7.generate(), orderId, from, target, eventId, eventType, applied, reason, now));
        if (applied) {
            log.info("order.status orderId={} {} -> {} by={}", orderId, from, target, eventType);
        } else {
            log.warn("order.status.out_of_order orderId={} status={} event={} eventId={}", orderId, from, eventType, eventId);
        }
    }
}
