package com.mktplace.commons.it;

import com.mktplace.commons.audit.AuditKind;
import com.mktplace.commons.audit.AuditRecord;
import com.mktplace.commons.audit.ConsumeStatus;
import com.mktplace.commons.consumer.ProcessedEventStore;
import com.mktplace.commons.events.OrderCreated;
import com.mktplace.commons.util.UuidV7;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class IdempotencyIT extends AbstractKafkaIT {

    @Autowired
    ProcessedEventStore processedEvents;

    @Test
    void mesmaMensagemEntregueDuasVezesGeraEfeitoUmaVez() {
        OrderCreated order = newOrder();
        UUID eventId = UuidV7.generate();
        ProducerRecord<String, String> record = publisher.toRecord(order, eventId, "corr-idem", null);

        publisher.send(record).join();
        publisher.send(record).join();   // republicação (ex.: outbox após queda entre send e update)

        await().atMost(TIMEOUT).untilAsserted(() -> {
            List<AuditRecord> consumed = audit.forEvent(eventId.toString(), AuditKind.CONSUMED);
            assertThat(consumed).hasSize(2);
            assertThat(consumed).extracting(audit -> audit.status()).containsOnly(ConsumeStatus.SUCCESS);
        });
        assertThat(listener.timesProcessed(order.orderId())).isEqualTo(1);
        assertThat(processedEvents.wasProcessed(eventId, TestOrderListener.GROUP)).isTrue();
    }
}
