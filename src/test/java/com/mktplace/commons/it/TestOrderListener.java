package com.mktplace.commons.it;

import com.mktplace.commons.consumer.IdempotencyGuard;
import com.mktplace.commons.events.EventCodec;
import com.mktplace.commons.events.OrderCreated;
import com.mktplace.commons.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/** Listener de negócio de mentira: registra cada pedido processado (uma vez, graças ao IdempotencyGuard). */
@Component
public class TestOrderListener {

    public static final String LISTENER_ID = "test-orders";
    public static final String GROUP = "test-service-v1";

    private final EventCodec codec;
    private final IdempotencyGuard guard;
    private final List<UUID> processed = new CopyOnWriteArrayList<>();

    public TestOrderListener(EventCodec codec, IdempotencyGuard guard) {
        this.codec = codec;
        this.guard = guard;
    }

    @KafkaListener(id = LISTENER_ID, topics = Topics.ORDERS_CREATED, groupId = GROUP)
    public void onOrderCreated(ConsumerRecord<String, String> record, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        OrderCreated event = codec.decode(record, OrderCreated.class);
        guard.runOnce(record, groupId, () -> processed.add(event.orderId()));
    }

    public long timesProcessed(UUID orderId) {
        return processed.stream().filter(id -> orderId.equals(id)).count();
    }
}
