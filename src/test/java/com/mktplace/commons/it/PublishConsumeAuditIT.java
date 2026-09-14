package com.mktplace.commons.it;

import com.mktplace.commons.audit.AuditKind;
import com.mktplace.commons.audit.AuditRecord;
import com.mktplace.commons.audit.ConsumeStatus;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.OrderCreated;
import com.mktplace.commons.events.PublishedEvent;
import com.mktplace.commons.events.Topics;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class PublishConsumeAuditIT extends AbstractKafkaIT {

    @Test
    void publicaComHeadersObrigatoriosEAuditaProducaoEConsumo() {
        OrderCreated order = newOrder();
        String correlationId = "corr-" + order.orderId();

        PublishedEvent published = publisher.publish(order, correlationId, null).join();

        assertThat(published.topic()).isEqualTo(Topics.ORDERS_CREATED);
        assertThat(published.correlationId()).isEqualTo(correlationId);
        assertThat(published.causationId()).isNull();

        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(listener.timesProcessed(order.orderId())).isEqualTo(1));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            List<AuditRecord> produced = audit.forEvent(published.eventId(), AuditKind.PRODUCED);
            assertThat(produced).hasSize(1);
            AuditRecord p = produced.getFirst();
            assertThat(p.service()).isEqualTo("test-service");
            assertThat(p.producer()).isEqualTo("test-service");
            assertThat(p.topic()).isEqualTo(Topics.ORDERS_CREATED);
            assertThat(p.partition()).isEqualTo(published.partition());
            assertThat(p.offset()).isEqualTo(published.offset());
            assertThat(p.key()).isEqualTo(order.orderId().toString());
            assertThat(p.orderId()).isEqualTo(order.orderId().toString());
            assertThat(p.correlationId()).isEqualTo(correlationId);
            assertThat(p.schemaVersion()).isEqualTo(1);
            assertThat(p.producedAt()).isNotNull();
            assertThat(p.payload()).contains("SKU-123");
            assertThat(p.headers()).containsKeys(EventHeaders.EVENT_ID, EventHeaders.EVENT_TYPE,
                    EventHeaders.SCHEMA_VERSION, EventHeaders.CORRELATION_ID, EventHeaders.PRODUCER,
                    EventHeaders.PRODUCED_AT, EventHeaders.ORDER_ID, EventHeaders.TRACEPARENT);

            List<AuditRecord> consumed = audit.forEvent(published.eventId(), AuditKind.CONSUMED);
            assertThat(consumed).hasSize(1);
            AuditRecord c = consumed.getFirst();
            assertThat(c.consumerGroup()).isEqualTo(TestOrderListener.GROUP);
            assertThat(c.status()).isEqualTo(ConsumeStatus.SUCCESS);
            assertThat(c.attempt()).isEqualTo(1);
            assertThat(c.processingTimeMs()).isNotNull().isGreaterThanOrEqualTo(0);
            assertThat(c.consumedAt()).isNotNull();
            assertThat(c.errorMessage()).isNull();
            assertThat(c.payload()).isNull();
        });
    }
}
