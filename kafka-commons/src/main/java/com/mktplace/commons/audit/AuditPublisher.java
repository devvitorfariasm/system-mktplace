package com.mktplace.commons.audit;

import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.HeaderCodec;
import com.mktplace.commons.events.Topics;
import com.mktplace.commons.util.UuidV7;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Map;

/**
 * Escreve em event-audit-log. Envia direto pelo KafkaTemplate (não pelo EventPublisher) para não auditar
 * a própria auditoria. Falha de auditoria é logada, nunca derruba o fluxo de negócio.
 */
@Component
public class AuditPublisher {

    private static final Logger log = LoggerFactory.getLogger(AuditPublisher.class);

    private final KafkaTemplate<String, String> template;
    private final JsonMapper mapper;
    private final MktplaceProperties props;
    private final Clock clock;

    public AuditPublisher(KafkaTemplate<String, String> template, JsonMapper mapper, MktplaceProperties props, Clock clock) {
        this.template = template;
        this.mapper = mapper;
        this.props = props;
        this.clock = clock;
    }

    public void produced(ProducerRecord<String, String> record, RecordMetadata metadata) {
        if (!Topics.isAuditable(record.topic())) {
            return;
        }
        Map<String, String> headers = HeaderCodec.toMap(record.headers());
        send(base(headers, AuditKind.PRODUCED)
                .topic(metadata.topic()).partition(metadata.partition()).offset(metadata.offset())
                .key(record.key()).payload(record.value())
                .build());
    }

    public void consumed(ConsumerRecord<?, ?> record, String consumerGroup, OffsetDateTime consumedAt,
                         long processingTimeMs, int attempt, ConsumeStatus status, @Nullable Throwable error) {
        if (!Topics.isAuditable(record.topic())) {
            return;
        }
        Map<String, String> headers = HeaderCodec.toMap(record.headers());
        send(base(headers, AuditKind.CONSUMED)
                .topic(record.topic()).partition(record.partition()).offset(record.offset())
                .key(record.key() == null ? null : record.key().toString())
                .consumerGroup(consumerGroup).consumedAt(consumedAt).processingTimeMs(processingTimeMs)
                .attempt(attempt).status(status)
                .errorClass(error == null ? null : error.getClass().getName())
                .errorMessage(error == null ? null : error.getMessage())
                .build());
    }

    /** Descarte manual de uma mensagem da DLQ: aparece na linha do tempo do pedido com o motivo. */
    public void discarded(Map<String, String> originalHeaders, String originalTopic, String dlqMessageId,
                          @Nullable String key, String reason) {
        send(base(originalHeaders, AuditKind.DLQ_DISCARDED)
                .topic(originalTopic).key(key)
                .consumerGroup(originalHeaders.get(org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP))
                .status(ConsumeStatus.DLQ)
                .errorClass("DlqMessageDiscarded").errorMessage(reason + " (dlqMessageId=" + dlqMessageId + ")")
                .build());
    }

    private AuditRecord.AuditRecordBuilder base(Map<String, String> headers, AuditKind kind) {
        Integer schemaVersion = null;
        try {
            schemaVersion = headers.containsKey(EventHeaders.SCHEMA_VERSION)
                    ? Integer.valueOf(headers.get(EventHeaders.SCHEMA_VERSION)) : null;
        } catch (NumberFormatException ignored) {
            // header inválido: registra sem versão em vez de perder o registro
        }
        return AuditRecord.builder()
                .recordId(UuidV7.generate())
                .kind(kind)
                .service(props.serviceName())
                .eventId(headers.get(EventHeaders.EVENT_ID))
                .eventType(headers.get(EventHeaders.EVENT_TYPE))
                .orderId(headers.get(EventHeaders.ORDER_ID))
                .correlationId(headers.get(EventHeaders.CORRELATION_ID))
                .causationId(headers.get(EventHeaders.CAUSATION_ID))
                .schemaVersion(schemaVersion)
                .producer(headers.get(EventHeaders.PRODUCER))
                .producedAt(parse(headers.get(EventHeaders.PRODUCED_AT)))
                .headers(headers)
                .recordedAt(OffsetDateTime.now(clock).atZoneSameInstant(props.zone()).toOffsetDateTime());
    }

    private void send(AuditRecord record) {
        String key = record.orderId() != null ? record.orderId() : record.key();
        try {
            template.send(new ProducerRecord<>(Topics.AUDIT_LOG, key, mapper.writeValueAsString(record)))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.warn("audit.publish_failed kind={} eventId={} topic={}: {}",
                                    record.kind(), record.eventId(), record.topic(), error.toString());
                        }
                    });
        } catch (RuntimeException e) {
            log.warn("audit.publish_failed kind={} eventId={}: {}", record.kind(), record.eventId(), e.toString());
        }
    }

    private static @Nullable OffsetDateTime parse(@Nullable String iso) {
        if (iso == null) {
            return null;
        }
        try {
            return OffsetDateTime.parse(iso);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
