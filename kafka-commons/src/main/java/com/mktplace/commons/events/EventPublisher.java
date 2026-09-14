package com.mktplace.commons.events;

import com.mktplace.commons.audit.AuditPublisher;
import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.util.UuidV7;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Único caminho de publicação de eventos de negócio. Monta os headers obrigatórios e, no ack do broker,
 * emite o registro PRODUCED da auditoria com partição/offset reais (ADR 0003).
 */
@Component
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, String> template;
    private final EventCodec codec;
    private final AuditPublisher audit;
    private final MktplaceProperties props;
    private final Clock clock;
    private final Executor callbackExecutor;

    public EventPublisher(KafkaTemplate<String, String> template, EventCodec codec, AuditPublisher audit,
                          MktplaceProperties props, Clock clock,
                          @Qualifier("kafkaCallbackExecutor") Executor callbackExecutor) {
        this.template = template;
        this.codec = codec;
        this.audit = audit;
        this.props = props;
        this.clock = clock;
        this.callbackExecutor = callbackExecutor;
    }

    /** Publica usando o contexto da thread (requisição HTTP ou evento em processamento). */
    public CompletableFuture<PublishedEvent> publish(DomainEvent event) {
        EventContext context = EventContextHolder.get();
        return publish(event, context.correlationIdOrNew(), context.causationId());
    }

    public CompletableFuture<PublishedEvent> publish(DomainEvent event, String correlationId, @Nullable String causationId) {
        return send(toRecord(event, UuidV7.generate(), correlationId, causationId));
    }

    /** Monta o record sem enviar: usado pelo outbox, que persiste headers e payload na transação do negócio. */
    public ProducerRecord<String, String> toRecord(DomainEvent event, UUID eventId, String correlationId, @Nullable String causationId) {
        var record = new ProducerRecord<>(Topics.forEventType(event.eventType()), event.key(), codec.encode(event));
        Headers headers = record.headers();
        put(headers, EventHeaders.EVENT_ID, eventId.toString());
        put(headers, EventHeaders.EVENT_TYPE, event.eventType());
        put(headers, EventHeaders.SCHEMA_VERSION, Integer.toString(event.schemaVersion()));
        put(headers, EventHeaders.CORRELATION_ID, correlationId);
        if (causationId != null) {
            put(headers, EventHeaders.CAUSATION_ID, causationId);
        }
        put(headers, EventHeaders.PRODUCER, props.serviceName());
        put(headers, EventHeaders.PRODUCED_AT, now());
        if (event.orderId() != null) {
            put(headers, EventHeaders.ORDER_ID, event.orderId().toString());
        }
        return record;
    }

    /** Envia um record já montado (outbox relay, reprocessamento de DLQ) e audita a produção. */
    public CompletableFuture<PublishedEvent> send(ProducerRecord<String, String> original) {
        // O producer fecha os headers do record após o envio; copiar permite reenviar o mesmo record (outbox, testes)
        var record = new ProducerRecord<>(original.topic(), original.partition(), original.key(), original.value());
        original.headers().forEach(h -> record.headers().add(h.key(), h.value()));
        // thenApplyAsync: o callback do producer roda na thread de rede do Kafka; publicar a auditoria ali
        // bloqueia a busca de metadata (deadlock até max.block.ms). A auditoria sai numa virtual thread.
        return template.send(record).thenApplyAsync(result -> {
            var metadata = result.getRecordMetadata();
            audit.produced(result.getProducerRecord(), metadata);
            var published = PublishedEvent.from(record, metadata);
            log.info("kafka.produced type={} topic={} partition={} offset={} eventId={} correlationId={}",
                    published.eventType(), published.topic(), published.partition(), published.offset(),
                    published.eventId(), published.correlationId());
            return published;
        }, callbackExecutor);
    }

    private String now() {
        return OffsetDateTime.now(clock).atZoneSameInstant(props.zone()).toOffsetDateTime()
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    private static void put(Headers headers, String name, String value) {
        headers.remove(name);
        headers.add(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
