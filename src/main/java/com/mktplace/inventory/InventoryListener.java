package com.mktplace.inventory;

import com.mktplace.commons.consumer.IdempotencyGuard;
import com.mktplace.commons.events.EventCodec;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.HeaderCodec;
import com.mktplace.commons.events.OrderCreated;
import com.mktplace.commons.events.PaymentApproved;
import com.mktplace.commons.events.StockReserved;
import com.mktplace.commons.events.StockUnavailable;
import com.mktplace.commons.events.Topics;
import com.mktplace.commons.outbox.OutboxPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Consome PaymentApproved e reserva o estoque. Os itens vêm da cópia local feita ao consumir
 * OrderCreated (grupo próprio), sem chamada síncrona ao order-service.
 */
@Component
@Profile("inventory")
public class InventoryListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryListener.class);

    private final ReservationService reservations;
    private final OrderItemsCache itemsCache;
    private final EventCodec codec;
    private final IdempotencyGuard guard;
    private final OutboxPublisher outbox;

    public InventoryListener(ReservationService reservations, OrderItemsCache itemsCache, EventCodec codec,
                             IdempotencyGuard guard, OutboxPublisher outbox) {
        this.reservations = reservations;
        this.itemsCache = itemsCache;
        this.codec = codec;
        this.guard = guard;
        this.outbox = outbox;
    }

    @KafkaListener(id = "inventory-orders", topics = Topics.ORDERS_CREATED, groupId = "inventory-service-orders-v1")
    public void onOrderCreated(ConsumerRecord<String, String> record, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        OrderCreated order = codec.decode(record, OrderCreated.class);
        guard.runOnce(record, groupId, () -> itemsCache.save(order));
    }

    @KafkaListener(id = "inventory-service", topics = Topics.PAYMENTS_APPROVED, groupId = "inventory-service-v1")
    public void onPaymentApproved(ConsumerRecord<String, String> record, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        PaymentApproved payment = codec.decode(record, PaymentApproved.class);
        String eventId = HeaderCodec.require(record.headers(), EventHeaders.EVENT_ID);
        UUID orderId = payment.orderId();
        List<OrderCreated.Item> items = itemsCache.find(orderId);   // pode lançar TransientFailure → retry
        guard.runOnce(record, groupId, () -> {
            ReservationService.Outcome outcome = reservations.reserve(orderId, items, eventId);
            switch (outcome) {
                case ReservationService.Reserved r -> {
                    outbox.enqueue(new StockReserved(orderId, r.reservations(), reservations.now()));
                    log.info("inventory.reserved orderId={} items={}", orderId, r.reservations().size());
                }
                case ReservationService.Unavailable u -> {
                    outbox.enqueue(new StockUnavailable(orderId, u.shortages(), reservations.now()));
                    log.warn("inventory.unavailable orderId={} shortages={}", orderId, u.shortages());
                }
            }
        });
    }
}
