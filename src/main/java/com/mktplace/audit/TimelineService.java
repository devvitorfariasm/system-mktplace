package com.mktplace.audit;

import com.mktplace.audit.AuditEventStore.Row;
import com.mktplace.audit.TimelineDtos.*;
import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.events.EventTypes;
import com.mktplace.commons.web.NotFoundException;
import org.jspecify.annotations.Nullable;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Monta a linha do tempo a partir de UMA query (AuditEventStore.findByOrder): agrupa por eventId,
 * consumidores por grupo, tentativas por número; deriva DLQ, gaps, caminho crítico e status atual.
 */
@Service
@Profile("audit")
public class TimelineService {

    private static final Map<String, String> STATUS_BY_EVENT = Map.of(
            EventTypes.ORDER_CREATED, "CREATED",
            EventTypes.PAYMENT_APPROVED, "PAID",
            EventTypes.PAYMENT_REJECTED, "PAYMENT_FAILED",
            EventTypes.STOCK_RESERVED, "CONFIRMED",
            EventTypes.STOCK_UNAVAILABLE, "OUT_OF_STOCK",
            EventTypes.ORDER_CANCELLED, "CANCELLED");

    private final AuditEventStore store;
    private final JsonMapper json;
    private final MktplaceProperties props;

    public TimelineService(AuditEventStore store, JsonMapper json, MktplaceProperties props) {
        this.store = store;
        this.json = json;
        this.props = props;
    }

    public Timeline build(UUID orderId) {
        List<Row> rows = store.findByOrder(orderId);
        if (rows.isEmpty()) {
            throw new NotFoundException("timeline-not-found", "Nenhum evento auditado para o pedido " + orderId);
        }
        // 1) evento por eventId, preservando a ordem de produção
        Map<String, Row> produced = new LinkedHashMap<>();
        Map<String, Map<String, List<Row>>> consumedByEventAndGroup = new LinkedHashMap<>();
        List<Row> discarded = new ArrayList<>();
        for (Row r : rows) {
            switch (r.kind()) {
                case "PRODUCED" -> produced.putIfAbsent(r.eventId(), r);
                case "CONSUMED" -> consumedByEventAndGroup.computeIfAbsent(r.eventId(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(r.consumerGroup(), k -> new ArrayList<>()).add(r);
                case "DLQ_DISCARDED" -> discarded.add(r);
                default -> { }
            }
        }
        // eventos consumidos cujo PRODUCED ainda não chegou (auditoria assíncrona): entram só com o que se sabe
        consumedByEventAndGroup.keySet().forEach(id -> produced.computeIfAbsent(id, k -> {
            Row any = consumedByEventAndGroup.get(k).values().iterator().next().getFirst();
            return any;
        }));

        List<Event> events = new ArrayList<>();
        List<DlqEntry> dlq = new ArrayList<>();
        long criticalPath = 0;
        String correlationId = null;
        OffsetDateTime finishedAt = null;
        for (Row p : produced.values()) {
            correlationId = correlationId == null ? p.correlationId() : correlationId;
            List<ConsumerEntry> consumers = new ArrayList<>();
            long slowest = 0;
            for (var byGroup : consumedByEventAndGroup.getOrDefault(p.eventId(), Map.of()).entrySet()) {
                List<Row> attempts = byGroup.getValue().stream()
                        .sorted(Comparator.comparing(r -> r.attempt() == null ? 0 : r.attempt())).toList();
                Row last = attempts.getLast();
                List<String> emitted = produced.values().stream()
                        .filter(child -> p.eventId().equals(child.causationId()) && last.service().equals(child.producer()))
                        .map(Row::eventId).toList();
                consumers.add(new ConsumerEntry(last.service(), byGroup.getKey(), attempts.stream().map(a ->
                        new Attempt(a.attempt() == null ? 1 : a.attempt(), zoned(a.consumedAt()), a.processingTimeMs(),
                                a.status(), a.errorClass(), a.errorMessage())).toList(), emitted));
                for (Row a : attempts) {
                    slowest = Math.max(slowest, a.processingTimeMs() == null ? 0 : a.processingTimeMs());
                    if ("DLQ".equals(a.status())) {
                        boolean wasDiscarded = discarded.stream().anyMatch(d -> p.eventId().equals(d.eventId()));
                        dlq.add(new DlqEntry(p.eventId(), p.eventType(), byGroup.getKey(), a.attempt() == null ? 1 : a.attempt(),
                                a.errorClass(), a.errorMessage(), zoned(a.consumedAt()), wasDiscarded));
                    }
                    if ("SUCCESS".equals(a.status()) && a.consumedAt() != null
                            && (finishedAt == null || a.consumedAt().isAfter(finishedAt))) {
                        finishedAt = a.consumedAt();
                    }
                }
            }
            criticalPath += slowest;
            events.add(new Event(p.eventId(), p.eventType(), p.schemaVersion(), p.topic(), p.partition(), p.offset(), p.key(),
                    p.producer(), zoned(p.producedAt()), p.causationId(), p.headers(), parse(p.payload()), consumers));
        }

        List<Gap> gaps = new ArrayList<>();
        for (int i = 1; i < events.size(); i++) {
            Event prev = events.get(i - 1);
            Event next = events.get(i);
            if (prev.producedAt() == null || next.producedAt() == null) {
                continue;
            }
            long waiting = Duration.between(prev.producedAt(), next.producedAt()).toMillis();
            long retries = prev.consumers().stream().flatMap(c -> c.attempts().stream())
                    .filter(a -> !"SUCCESS".equals(a.status())).count();
            String note = retries == 0 ? null : retries + " tentativa(s) com falha antes de " + next.eventType();
            gaps.add(new Gap(List.of(prev.eventId(), next.eventId()), waiting, note));
        }

        String currentStatus = events.stream().map(Event::eventType).filter(STATUS_BY_EVENT::containsKey)
                .reduce((a, b) -> b).map(STATUS_BY_EVENT::get).orElse("UNKNOWN");
        OffsetDateTime startedAt = events.getFirst().producedAt();
        Long total = startedAt == null || finishedAt == null ? null : Duration.between(startedAt, finishedAt).toMillis();
        return new Timeline(orderId, correlationId, currentStatus, startedAt, zoned(finishedAt), total, criticalPath,
                events, dlq, gaps);
    }

    private @Nullable Object parse(@Nullable String payload) {
        if (payload == null) {
            return null;
        }
        try {
            return json.readTree(payload);
        } catch (RuntimeException e) {
            return payload;
        }
    }

    private @Nullable OffsetDateTime zoned(@Nullable OffsetDateTime at) {
        return at == null ? null : at.atZoneSameInstant(props.zone()).toOffsetDateTime();
    }
}
