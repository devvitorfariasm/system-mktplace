package com.mktplace.commons.dlq;

import org.jspecify.annotations.Nullable;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/** Linha de dlq_messages: uma mensagem que chegou à DLT de um tópico consumido por este serviço. */
public record DlqMessage(
        UUID id,
        @Nullable UUID eventId,
        @Nullable String eventType,
        @Nullable String messageKey,
        String originalTopic,
        int originalPartition,
        long originalOffset,
        String consumerGroup,
        String dltTopic,
        int dltPartition,
        long dltOffset,
        @Nullable String exceptionClass,
        @Nullable String exceptionMessage,
        @Nullable String stackTrace,
        int attempts,
        Map<String, String> headers,
        @Nullable String payload,
        OffsetDateTime failedAt,
        DlqStatus status,
        @Nullable OffsetDateTime resolvedAt,
        @Nullable String resolutionReason,
        int reprocessCount) {

    private static final int STACK_TRACE_PREVIEW = 1500;

    /** Versão para listagem: stack trace truncado. */
    public DlqMessage summary() {
        if (stackTrace == null || stackTrace.length() <= STACK_TRACE_PREVIEW) {
            return this;
        }
        return new DlqMessage(id, eventId, eventType, messageKey, originalTopic, originalPartition, originalOffset,
                consumerGroup, dltTopic, dltPartition, dltOffset, exceptionClass, exceptionMessage,
                stackTrace.substring(0, STACK_TRACE_PREVIEW) + "… (truncado)", attempts, headers, payload,
                failedAt, status, resolvedAt, resolutionReason, reprocessCount);
    }
}
