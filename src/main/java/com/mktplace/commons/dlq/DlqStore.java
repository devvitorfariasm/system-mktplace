package com.mktplace.commons.dlq;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Persistência de dlq_messages (migration comum V0_002). */
@Repository
public class DlqStore {

    private static final String COLUMNS = """
            id, event_id, event_type, message_key, original_topic, original_partition, original_offset,
            consumer_group, dlt_topic, dlt_partition, dlt_offset, exception_class, exception_message, stack_trace,
            attempts, headers::text AS headers, payload, failed_at, status, resolved_at, resolution_reason, reprocess_count
            """;

    private final JdbcClient jdbc;
    private final RowMapper<DlqMessage> mapper;

    public DlqStore(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.mapper = (rs, rowNum) -> map(rs, json);
    }

    /** Idempotente por (dlt_topic, dlt_partition, dlt_offset): reentrega do listener interno não duplica. */
    public boolean insert(DlqMessage m, String headersJson) {
        return jdbc.sql("""
                INSERT INTO dlq_messages (id, event_id, event_type, message_key, original_topic, original_partition,
                    original_offset, consumer_group, dlt_topic, dlt_partition, dlt_offset, exception_class,
                    exception_message, stack_trace, attempts, headers, payload, failed_at, status)
                VALUES (:id, :eventId, :eventType, :key, :originalTopic, :originalPartition, :originalOffset, :group,
                    :dltTopic, :dltPartition, :dltOffset, :exceptionClass, :exceptionMessage, :stackTrace, :attempts,
                    CAST(:headers AS jsonb), :payload, :failedAt, 'PENDING')
                ON CONFLICT (dlt_topic, dlt_partition, dlt_offset) DO NOTHING
                """)
                .param("id", m.id()).param("eventId", m.eventId()).param("eventType", m.eventType())
                .param("key", m.messageKey()).param("originalTopic", m.originalTopic())
                .param("originalPartition", m.originalPartition()).param("originalOffset", m.originalOffset())
                .param("group", m.consumerGroup()).param("dltTopic", m.dltTopic())
                .param("dltPartition", m.dltPartition()).param("dltOffset", m.dltOffset())
                .param("exceptionClass", m.exceptionClass()).param("exceptionMessage", m.exceptionMessage())
                .param("stackTrace", m.stackTrace()).param("attempts", m.attempts())
                .param("headers", headersJson).param("payload", m.payload()).param("failedAt", m.failedAt())
                .update() == 1;
    }

    public List<DlqMessage> findAll(DlqStatus status, @Nullable String originalTopic, int limit) {
        String topicFilter = originalTopic == null ? "" : " AND original_topic = :topic";
        var spec = jdbc.sql("SELECT " + COLUMNS + " FROM dlq_messages WHERE status = :status" + topicFilter
                        + " ORDER BY failed_at DESC, id DESC LIMIT :limit")
                .param("status", status.name()).param("limit", limit);
        if (originalTopic != null) {
            spec = spec.param("topic", originalTopic);
        }
        return spec.query(mapper).list();
    }

    public Optional<DlqMessage> find(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM dlq_messages WHERE id = :id").param("id", id)
                .query(mapper).optional();
    }

    public long countPending() {
        return jdbc.sql("SELECT count(*) FROM dlq_messages WHERE status = 'PENDING'").query(Long.class).single();
    }

    public void markReprocessed(UUID id, OffsetDateTime at) {
        jdbc.sql("""
                UPDATE dlq_messages SET status = 'REPROCESSED', resolved_at = :at, reprocess_count = reprocess_count + 1
                WHERE id = :id
                """).param("id", id).param("at", at).update();
    }

    public void markDiscarded(UUID id, String reason, OffsetDateTime at) {
        jdbc.sql("UPDATE dlq_messages SET status = 'DISCARDED', resolved_at = :at, resolution_reason = :reason WHERE id = :id")
                .param("id", id).param("at", at).param("reason", reason).update();
    }

    @SuppressWarnings("unchecked")
    private static DlqMessage map(ResultSet rs, JsonMapper json) throws SQLException {
        return new DlqMessage(
                rs.getObject("id", UUID.class),
                rs.getObject("event_id", UUID.class),
                rs.getString("event_type"),
                rs.getString("message_key"),
                rs.getString("original_topic"),
                rs.getInt("original_partition"),
                rs.getLong("original_offset"),
                rs.getString("consumer_group"),
                rs.getString("dlt_topic"),
                rs.getInt("dlt_partition"),
                rs.getLong("dlt_offset"),
                rs.getString("exception_class"),
                rs.getString("exception_message"),
                rs.getString("stack_trace"),
                rs.getInt("attempts"),
                json.readValue(rs.getString("headers"), Map.class),
                rs.getString("payload"),
                rs.getObject("failed_at", OffsetDateTime.class),
                DlqStatus.valueOf(rs.getString("status")),
                rs.getObject("resolved_at", OffsetDateTime.class),
                rs.getString("resolution_reason"),
                rs.getInt("reprocess_count"));
    }
}
