package com.mktplace.commons.dlq;

import com.mktplace.commons.audit.AuditPublisher;
import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.EventPublisher;
import com.mktplace.commons.events.PublishedEvent;
import com.mktplace.commons.web.ConflictException;
import com.mktplace.commons.web.NotFoundException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Reprocessa (republica no tópico original com os headers originais + x-reprocessed-from/x-reprocess-attempt)
 * ou descarta (registro DlqMessageDiscarded na auditoria) mensagens da DLQ.
 */
@Service
public class DlqReprocessService {

    private static final Logger log = LoggerFactory.getLogger(DlqReprocessService.class);
    private static final int BATCH_LIMIT = 10_000;

    private final DlqStore store;
    private final EventPublisher publisher;
    private final AuditPublisher audit;
    private final ReprocessJobs jobs;
    private final MktplaceProperties props;
    private final Clock clock;

    public DlqReprocessService(DlqStore store, EventPublisher publisher, AuditPublisher audit, ReprocessJobs jobs,
                               MktplaceProperties props, Clock clock) {
        this.store = store;
        this.publisher = publisher;
        this.audit = audit;
        this.jobs = jobs;
        this.props = props;
        this.clock = clock;
    }

    public PublishedEvent reprocess(UUID id) {
        DlqMessage message = pending(id);
        var record = new ProducerRecord<>(message.originalTopic(), null, message.messageKey(), message.payload());
        // headers originais, sem os kafka_* e x-dlq-* que descrevem a falha anterior
        message.headers().forEach((name, value) -> {
            if (!name.startsWith("kafka_") && !name.startsWith(EventHeaders.DLQ_PREFIX)) {
                record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
            }
        });
        record.headers().remove(EventHeaders.REPROCESSED_FROM).remove(EventHeaders.REPROCESS_ATTEMPT);
        record.headers().add(EventHeaders.REPROCESSED_FROM, id.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(EventHeaders.REPROCESS_ATTEMPT,
                Integer.toString(message.reprocessCount() + 1).getBytes(StandardCharsets.UTF_8));
        PublishedEvent published = publisher.send(record).join();   // ack do broker antes de marcar
        store.markReprocessed(id, now());
        log.warn("dlq.reprocessed id={} eventId={} topic={} partition={} offset={}", id, published.eventId(),
                published.topic(), published.partition(), published.offset());
        return published;
    }

    /** Lote assíncrono: responde 202 com jobId; progresso em GET /admin/jobs/{jobId}. */
    public UUID reprocessAll(@Nullable String originalTopic) {
        List<DlqMessage> pending = store.findAll(DlqStatus.PENDING, originalTopic, BATCH_LIMIT);
        ReprocessJobs.Job job = jobs.create(originalTopic, pending.size());
        Thread.ofVirtual().name("dlq-reprocess-" + job.id()).start(() -> {
            for (DlqMessage message : pending) {
                try {
                    reprocess(message.id());
                    job.succeeded();
                } catch (RuntimeException e) {
                    log.warn("dlq.reprocess_failed id={}: {}", message.id(), e.toString());
                    job.failed();
                }
            }
            jobs.finish(job);
        });
        return job.id();
    }

    public void discard(UUID id, String reason) {
        DlqMessage message = pending(id);
        store.markDiscarded(id, reason, now());
        audit.discarded(message.headers(), message.originalTopic(), id.toString(), message.messageKey(), reason);
        log.warn("dlq.discarded id={} eventId={} reason={}", id, message.eventId(), reason);
    }

    private DlqMessage pending(UUID id) {
        DlqMessage message = store.find(id)
                .orElseThrow(() -> new NotFoundException("dlq-message-not-found", "Mensagem de DLQ não encontrada: " + id));
        if (message.status() != DlqStatus.PENDING) {
            throw new ConflictException("dlq-message-already-resolved",
                    "Mensagem %s já está %s".formatted(id, message.status()));
        }
        return message;
    }

    private OffsetDateTime now() {
        return OffsetDateTime.now(clock).atZoneSameInstant(props.zone()).toOffsetDateTime();
    }
}
