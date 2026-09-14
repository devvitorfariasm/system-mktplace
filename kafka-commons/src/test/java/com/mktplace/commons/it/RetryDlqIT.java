package com.mktplace.commons.it;

import com.mktplace.commons.audit.AuditKind;
import com.mktplace.commons.audit.AuditRecord;
import com.mktplace.commons.audit.ConsumeStatus;
import com.mktplace.commons.chaos.ChaosMode;
import com.mktplace.commons.dlq.DlqMessage;
import com.mktplace.commons.dlq.DlqReprocessService;
import com.mktplace.commons.dlq.DlqStatus;
import com.mktplace.commons.dlq.DlqStore;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.OrderCreated;
import com.mktplace.commons.events.PublishedEvent;
import com.mktplace.commons.events.Topics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class RetryDlqIT extends AbstractKafkaIT {

    @Autowired
    DlqStore dlqStore;
    @Autowired
    DlqReprocessService reprocess;

    @Test
    void falhaTransitoriaPassaPeloRetryEAparecemTresTentativasNaAuditoria() {
        chaos.failNext(2, ChaosMode.TRANSIENT);
        OrderCreated order = newOrder();

        PublishedEvent published = publisher.publish(order, "corr-retry", null).join();

        await().atMost(TIMEOUT).untilAsserted(() -> {
            List<AuditRecord> consumed = audit.forEvent(published.eventId(), AuditKind.CONSUMED);
            assertThat(consumed).extracting(audit -> audit.status())
                    .containsExactly(ConsumeStatus.RETRY, ConsumeStatus.RETRY, ConsumeStatus.SUCCESS);
            assertThat(consumed).extracting(audit -> audit.attempt()).containsExactly(1, 2, 3);
            assertThat(consumed.getFirst().errorMessage()).contains("chaos");
            assertThat(consumed.getFirst().errorClass()).contains("TransientFailureException");
        });
        assertThat(listener.timesProcessed(order.orderId())).isEqualTo(1);
    }

    @Test
    void esgotarTentativasVaiParaDltComRegistroDlqNaAuditoriaENaTabela() {
        chaos.failNext(4, ChaosMode.TRANSIENT);   // maxAttempts=4: 3 RETRY + 1 DLQ
        OrderCreated order = newOrder();

        PublishedEvent published = publisher.publish(order, "corr-exhaust", null).join();

        await().atMost(TIMEOUT).untilAsserted(() -> {
            List<AuditRecord> consumed = audit.forEvent(published.eventId(), AuditKind.CONSUMED);
            assertThat(consumed).extracting(audit -> audit.status())
                    .containsExactly(ConsumeStatus.RETRY, ConsumeStatus.RETRY, ConsumeStatus.RETRY, ConsumeStatus.DLQ);
        });
        await().atMost(TIMEOUT).untilAsserted(() -> {
            List<DlqMessage> pending = dlqStore.findAll(DlqStatus.PENDING, Topics.ORDERS_CREATED, 100);
            assertThat(pending).anySatisfy(m -> {
                assertThat(m.eventId()).isEqualTo(UUID.fromString(published.eventId()));
                assertThat(m.originalTopic()).isEqualTo(Topics.ORDERS_CREATED);
                assertThat(m.originalPartition()).isEqualTo(published.partition());
                assertThat(m.originalOffset()).isEqualTo(published.offset());
                assertThat(m.consumerGroup()).isEqualTo(TestOrderListener.GROUP);
                assertThat(m.attempts()).isEqualTo(4);
                assertThat(m.dltTopic()).isEqualTo(Topics.dlt(Topics.ORDERS_CREATED));
                assertThat(m.exceptionClass()).contains("TransientFailureException");
                assertThat(m.stackTrace()).isNotBlank();
                assertThat(m.payload()).contains(order.orderId().toString());
            });
        });
        assertThat(listener.timesProcessed(order.orderId())).isZero();
    }

    @Test
    void poisonVaiDiretoParaDlqEReprocessarRepublicaComHeadersOriginais() {
        chaos.failNext(1, ChaosMode.POISON);
        OrderCreated order = newOrder();

        PublishedEvent published = publisher.publish(order, "corr-poison", null).join();

        await().atMost(TIMEOUT).untilAsserted(() -> {
            List<AuditRecord> consumed = audit.forEvent(published.eventId(), AuditKind.CONSUMED);
            assertThat(consumed).extracting(audit -> audit.status()).containsExactly(ConsumeStatus.DLQ);
            assertThat(consumed.getFirst().attempt()).isEqualTo(1);
        });
        DlqMessage dead = await().atMost(TIMEOUT).until(
                () -> dlqStore.findAll(DlqStatus.PENDING, Topics.ORDERS_CREATED, 100).stream()
                        .filter(m -> published.eventId().equals(String.valueOf(m.eventId()))).findFirst(),
                found -> found.isPresent()).orElseThrow();
        assertThat(dead.exceptionClass()).contains("PoisonMessageException");
        assertThat(listener.timesProcessed(order.orderId())).isZero();

        PublishedEvent republished = reprocess.reprocess(dead.id());

        assertThat(republished.eventId()).isEqualTo(published.eventId());
        assertThat(republished.topic()).isEqualTo(Topics.ORDERS_CREATED);
        assertThat(republished.correlationId()).isEqualTo("corr-poison");
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(listener.timesProcessed(order.orderId())).isEqualTo(1));
        await().atMost(TIMEOUT).untilAsserted(() -> {
            List<AuditRecord> produced = audit.forEvent(published.eventId(), AuditKind.PRODUCED);
            assertThat(produced).hasSize(2);
            AuditRecord second = produced.getLast();
            assertThat(second.headers()).containsEntry(EventHeaders.REPROCESSED_FROM, dead.id().toString())
                    .containsEntry(EventHeaders.REPROCESS_ATTEMPT, "1")
                    .containsEntry(EventHeaders.CORRELATION_ID, "corr-poison")
                    .doesNotContainKey("kafka_dlt-original-topic");
        });
        assertThat(dlqStore.find(dead.id()).orElseThrow().status()).isEqualTo(DlqStatus.REPROCESSED);
    }
}
