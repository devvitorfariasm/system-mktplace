package com.mktplace.commons.audit;

import com.mktplace.commons.chaos.ChaosService;
import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.consumer.RetryPolicy;
import com.mktplace.commons.events.EventContext;
import com.mktplace.commons.events.EventContextHolder;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.HeaderCodec;
import com.mktplace.commons.events.Topics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.OffsetDateTime;

/**
 * Auditoria de consumo transparente para os listeners (ADR 0003). Por tentativa de entrega:
 * intercept() marca o início, propaga correlation/causation para a thread e aplica o chaos;
 * success()/failure() calculam o tempo e classificam SUCCESS / RETRY / DLQ com a mesma regra
 * do DefaultErrorHandler (RetryPolicy + maxAttempts).
 */
@Component
public class AuditRecordInterceptor implements RecordInterceptor<Object, Object> {

    private static final Logger log = LoggerFactory.getLogger(AuditRecordInterceptor.class);
    private static final ThreadLocal<Attempt> CURRENT = new ThreadLocal<>();

    private record Attempt(long startNanos, OffsetDateTime consumedAt, int number) {
    }

    private final AuditPublisher audit;
    private final ChaosService chaos;
    private final ListenerStats stats;
    private final MktplaceProperties props;
    private final Clock clock;

    public AuditRecordInterceptor(AuditPublisher audit, ChaosService chaos, ListenerStats stats,
                                  MktplaceProperties props, Clock clock) {
        this.audit = audit;
        this.chaos = chaos;
        this.stats = stats;
        this.props = props;
        this.clock = clock;
    }

    @Override
    public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        var headers = record.headers();
        MDC.put("topic", record.topic());
        MDC.put("partition", Integer.toString(record.partition()));
        MDC.put("offset", Long.toString(record.offset()));
        String orderId = HeaderCodec.string(headers, EventHeaders.ORDER_ID);
        if (orderId != null) {
            MDC.put("orderId", orderId);
        }
        EventContextHolder.set(new EventContext(
                HeaderCodec.string(headers, EventHeaders.CORRELATION_ID),
                HeaderCodec.string(headers, EventHeaders.EVENT_ID)));
        int attempt = HeaderCodec.deliveryAttempt(headers);
        CURRENT.set(new Attempt(System.nanoTime(), OffsetDateTime.now(clock).atZoneSameInstant(props.zone()).toOffsetDateTime(), attempt));
        stats.started();
        if (Topics.isAuditable(record.topic())) {
            chaos.beforeProcessing(record);   // pode lançar: cai em failure() e no error handler
        }
        return record;
    }

    @Override
    public void success(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        finish(record, consumer, null);
    }

    @Override
    public void failure(ConsumerRecord<Object, Object> record, Exception exception, Consumer<Object, Object> consumer) {
        finish(record, consumer, exception);
    }

    private void finish(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer, @Nullable Exception error) {
        Attempt attempt = CURRENT.get();
        CURRENT.remove();
        try {
            if (attempt == null) {
                return;
            }
            long elapsedMs = (System.nanoTime() - attempt.startNanos()) / 1_000_000;
            stats.finished(error == null);
            if (!Topics.isAuditable(record.topic())) {
                return;
            }
            ConsumeStatus status = error == null ? ConsumeStatus.SUCCESS : classify(error, attempt.number());
            Throwable cause = error == null ? null : RetryPolicy.rootCause(error);
            String group = consumer.groupMetadata().groupId();
            audit.consumed(record, group, attempt.consumedAt(), elapsedMs, attempt.number(), status, cause);
            if (status == ConsumeStatus.SUCCESS) {
                log.info("kafka.consumed group={} attempt={} status={} took={}ms", group, attempt.number(), status, elapsedMs);
            } else {
                log.warn("kafka.consumed group={} attempt={} status={} took={}ms error={}", group, attempt.number(), status,
                        elapsedMs, cause);
            }
        } finally {
            EventContextHolder.clear();
            MDC.remove("topic");
            MDC.remove("partition");
            MDC.remove("offset");
            MDC.remove("orderId");
        }
    }

    ConsumeStatus classify(Exception error, int attempt) {
        if (RetryPolicy.isNonRetryable(error) || attempt >= props.kafka().maxAttempts()) {
            return ConsumeStatus.DLQ;
        }
        return ConsumeStatus.RETRY;
    }
}
