package com.mktplace.commons.dlq;

import com.mktplace.commons.chaos.ChaosService;
import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.HeaderCodec;
import com.mktplace.commons.events.Topics;
import com.mktplace.commons.util.UuidV7;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * Materializa as DLTs na tabela dlq_messages. Assina todas as "*.DLT" mas guarda só as mensagens cujo
 * consumer group original pertence a este serviço (header kafka_dlt-original-consumer-group).
 * Listener interno: não pausa com o chaos e não é auditado.
 */
@Component
public class DlqStoreListener {

    private static final Logger log = LoggerFactory.getLogger(DlqStoreListener.class);

    private final DlqStore store;
    private final ChaosService chaos;
    private final JsonMapper json;
    private final MktplaceProperties props;
    private final Clock clock;

    public DlqStoreListener(DlqStore store, ChaosService chaos, JsonMapper json, MktplaceProperties props, Clock clock) {
        this.store = store;
        this.chaos = chaos;
        this.json = json;
        this.props = props;
        this.clock = clock;
    }

    @KafkaListener(id = ChaosService.INTERNAL_LISTENER_PREFIX + "dlq-store",
            topicPattern = ".*\\.DLT",
            groupId = "${mktplace.service-name}-dlq-store")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        Headers h = record.headers();
        String group = HeaderCodec.string(h, EventHeaders.DLQ_CONSUMER_GROUP);
        if (group == null) {
            group = HeaderCodec.string(h, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP);
        }
        Integer attempts = HeaderCodec.intValue(h, EventHeaders.DLQ_ATTEMPTS);
        if (group != null && !chaos.ownsGroup(group)) {
            return;   // DLT de outro serviço
        }
        Map<String, String> headers = HeaderCodec.toMap(h);
        String originalTopic = headers.getOrDefault(KafkaHeaders.DLT_ORIGINAL_TOPIC,
                record.topic().substring(0, record.topic().length() - Topics.DLT_SUFFIX.length()));
        Integer originalPartition = HeaderCodec.intValue(h, KafkaHeaders.DLT_ORIGINAL_PARTITION);
        Long originalOffset = HeaderCodec.longValue(h, KafkaHeaders.DLT_ORIGINAL_OFFSET);
        String exceptionClass = headers.getOrDefault(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN,
                headers.get(KafkaHeaders.DLT_EXCEPTION_FQCN));
        var message = new DlqMessage(
                UuidV7.generate(), parseUuid(headers.get(EventHeaders.EVENT_ID)), headers.get(EventHeaders.EVENT_TYPE),
                record.key(), originalTopic,
                originalPartition == null ? record.partition() : originalPartition,
                originalOffset == null ? -1 : originalOffset,
                group == null ? props.serviceName() : group,
                record.topic(), record.partition(), record.offset(),
                exceptionClass, headers.get(KafkaHeaders.DLT_EXCEPTION_MESSAGE), headers.get(KafkaHeaders.DLT_EXCEPTION_STACKTRACE),
                attempts == null ? HeaderCodec.deliveryAttempt(h) : attempts, headers, record.value(),
                OffsetDateTime.now(clock).atZoneSameInstant(props.zone()).toOffsetDateTime(),
                DlqStatus.PENDING, null, null, 0);
        if (store.insert(message, json.writeValueAsString(headers))) {
            log.warn("dlq.stored id={} eventId={} originalTopic={} group={} error={}", message.id(), message.eventId(),
                    originalTopic, message.consumerGroup(), exceptionClass);
        }
    }

    private static @Nullable UUID parseUuid(@Nullable String raw) {
        try {
            return raw == null ? null : UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
