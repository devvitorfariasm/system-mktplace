package com.mktplace.commons.outbox;

import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.EventPublisher;
import com.mktplace.commons.events.Topics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Relay agendado do outbox (ADR 0002): a cada 500 ms publica o lote pendente em ordem, espera o ack
 * (acks=all) e marca published_at. Queda entre o ack e o UPDATE gera republicação com o mesmo eventId,
 * que os consumidores ignoram pela idempotência.
 */
@Component
@ConditionalOnProperty(name = "mktplace.outbox.relay-enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 100;

    private final OutboxStore store;
    private final EventPublisher publisher;
    private final MktplaceProperties props;
    private final Clock clock;

    public OutboxRelay(OutboxStore store, EventPublisher publisher, MktplaceProperties props, Clock clock) {
        this.store = store;
        this.publisher = publisher;
        this.props = props;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${mktplace.outbox.relay-delay-ms:500}")
    @Transactional
    public int relay() {
        var pending = store.lockPending(BATCH);
        int published = 0;
        for (var event : pending) {
            try {
                var result = publisher.send(toRecord(event)).get(10, TimeUnit.SECONDS);
                store.markPublished(event.id(), OffsetDateTime.now(clock));
                published++;
                log.debug("outbox.published eventId={} topic={} offset={}", result.eventId(), result.topic(), result.offset());
            } catch (ExecutionException | TimeoutException e) {
                store.markFailed(event.id(), String.valueOf(e.getCause() == null ? e : e.getCause()));
                log.warn("outbox.publish_failed eventId={} type={}: {}", event.id(), event.eventType(), e.toString());
                break;   // preserva a ordem: não pula o evento que falhou
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return published;
    }

    private ProducerRecord<String, String> toRecord(OutboxStore.PendingEvent e) {
        var record = new ProducerRecord<>(Topics.forEventType(e.eventType()), e.messageKey(), e.payload());
        var h = record.headers();
        h.add(EventHeaders.EVENT_ID, bytes(e.id().toString()));
        h.add(EventHeaders.EVENT_TYPE, bytes(e.eventType()));
        h.add(EventHeaders.SCHEMA_VERSION, bytes(Integer.toString(e.schemaVersion())));
        h.add(EventHeaders.CORRELATION_ID, bytes(e.correlationId()));
        if (e.causationId() != null) {
            h.add(EventHeaders.CAUSATION_ID, bytes(e.causationId()));
        }
        h.add(EventHeaders.PRODUCER, bytes(props.serviceName()));
        h.add(EventHeaders.PRODUCED_AT, bytes(OffsetDateTime.now(clock).atZoneSameInstant(props.zone())
                .toOffsetDateTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
        if (e.orderId() != null) {
            h.add(EventHeaders.ORDER_ID, bytes(e.orderId().toString()));
        }
        return record;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
