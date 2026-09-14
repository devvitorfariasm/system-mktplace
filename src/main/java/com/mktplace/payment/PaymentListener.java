package com.mktplace.payment;

import com.mktplace.commons.consumer.IdempotencyGuard;
import com.mktplace.commons.events.EventCodec;
import com.mktplace.commons.events.OrderCreated;
import com.mktplace.commons.events.PaymentApproved;
import com.mktplace.commons.events.PaymentRejected;
import com.mktplace.commons.events.Topics;
import com.mktplace.commons.outbox.OutboxPublisher;
import com.mktplace.commons.util.UuidV7;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.concurrent.ThreadLocalRandom;

/** Consome OrderCreated, simula o pagamento (~80% aprovação, 1 a 3 s) e publica o resultado pelo outbox. */
@Component
@Profile("payment")
@EnableConfigurationProperties(PaymentProperties.class)
public class PaymentListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentListener.class);

    private final PaymentRepository payments;
    private final EventCodec codec;
    private final IdempotencyGuard guard;
    private final OutboxPublisher outbox;
    private final PaymentProperties props;
    private final Clock clock;

    public PaymentListener(PaymentRepository payments, EventCodec codec, IdempotencyGuard guard, OutboxPublisher outbox,
                           PaymentProperties props, Clock clock) {
        this.payments = payments;
        this.codec = codec;
        this.guard = guard;
        this.outbox = outbox;
        this.props = props;
        this.clock = clock;
    }

    @KafkaListener(id = "payment-service", topics = Topics.ORDERS_CREATED, groupId = "payment-service-v1")
    public void onOrderCreated(ConsumerRecord<String, String> record, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        OrderCreated order = codec.decode(record, OrderCreated.class);
        long started = System.nanoTime();
        boolean approved = simulateGateway();
        long latency = (System.nanoTime() - started) / 1_000_000;
        guard.runOnce(record, groupId, () -> {
            var now = OffsetDateTime.now(clock);
            var payment = new Payment(UuidV7.generate(), order.orderId(), order.total(), order.currency(),
                    approved ? Payment.Status.APPROVED : Payment.Status.REJECTED,
                    approved ? null : "Recusado pelo emissor (simulado)", latency,
                    com.mktplace.commons.events.HeaderCodec.require(record.headers(), com.mktplace.commons.events.EventHeaders.EVENT_ID), now);
            payments.save(payment);
            if (approved) {
                outbox.enqueue(new PaymentApproved(order.orderId(), payment.getId(), order.total(), order.currency(), now));
            } else {
                outbox.enqueue(new PaymentRejected(order.orderId(), payment.getId(), order.total(), order.currency(),
                        payment.getReason(), now));
            }
            log.info("payment.processed orderId={} paymentId={} status={} latencyMs={}", order.orderId(), payment.getId(),
                    payment.getStatus(), latency);
        });
    }

    private boolean simulateGateway() {
        var random = ThreadLocalRandom.current();
        long wait = props.latencyMaxMs() <= props.latencyMinMs() ? props.latencyMinMs()
                : random.nextLong(props.latencyMinMs(), props.latencyMaxMs() + 1);
        if (wait > 0) {
            try {
                Thread.sleep(wait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return random.nextDouble() < props.approvalRate();
    }
}
