package com.mktplace.commons.audit;

import lombok.Builder;
import org.jspecify.annotations.Nullable;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * Registro publicado em event-audit-log por todo produtor e consumidor. Desnormalizado de propósito:
 * o audit-service grava como está e a linha do tempo é montada por (orderId, producedAt).
 */
@Builder
public record AuditRecord(
        UUID recordId,
        AuditKind kind,
        String service,
        @Nullable String eventId,
        @Nullable String eventType,
        String topic,
        @Nullable Integer partition,
        @Nullable Long offset,
        @Nullable String key,
        @Nullable String orderId,
        @Nullable String correlationId,
        @Nullable String causationId,
        @Nullable Integer schemaVersion,
        @Nullable String producer,
        @Nullable OffsetDateTime producedAt,
        Map<String, String> headers,
        @Nullable String payload,
        @Nullable String consumerGroup,
        @Nullable OffsetDateTime consumedAt,
        @Nullable Long processingTimeMs,
        @Nullable Integer attempt,
        @Nullable ConsumeStatus status,
        @Nullable String errorClass,
        @Nullable String errorMessage,
        OffsetDateTime recordedAt) {
}
