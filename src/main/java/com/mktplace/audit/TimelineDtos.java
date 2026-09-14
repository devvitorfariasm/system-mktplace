package com.mktplace.audit;

import org.jspecify.annotations.Nullable;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Resposta de GET /audit/orders/{orderId}/timeline (seção 6.7). */
public final class TimelineDtos {

    private TimelineDtos() {
    }

    public record Attempt(int attempt, @Nullable OffsetDateTime consumedAt, @Nullable Long processingTimeMs, String status,
                          @Nullable String errorClass, @Nullable String errorMessage) {
    }

    public record ConsumerEntry(String service, String consumerGroup, List<Attempt> attempts, List<String> emitted) {
    }

    public record Event(String eventId, @Nullable String eventType, @Nullable Integer schemaVersion, String topic,
                        @Nullable Integer partition, @Nullable Long offset, @Nullable String key, @Nullable String producer,
                        @Nullable OffsetDateTime producedAt, @Nullable String causationId, Map<String, String> headers,
                        @Nullable Object payload, List<ConsumerEntry> consumers) {
    }

    public record DlqEntry(String eventId, @Nullable String eventType, String consumerGroup, int attempt,
                           @Nullable String errorClass, @Nullable String errorMessage, @Nullable OffsetDateTime failedAt,
                           boolean discarded) {
    }

    public record Gap(List<String> between, long waitingMs, @Nullable String note) {
    }

    public record Timeline(UUID orderId, @Nullable String correlationId, String currentStatus,
                           @Nullable OffsetDateTime startedAt, @Nullable OffsetDateTime finishedAt,
                           @Nullable Long totalDurationMs, long criticalPathMs, List<Event> events, List<DlqEntry> dlq,
                           List<Gap> gaps) {
    }
}
