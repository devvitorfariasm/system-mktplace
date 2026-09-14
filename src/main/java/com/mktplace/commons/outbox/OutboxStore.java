package com.mktplace.commons.outbox;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/** Tabela outbox (migration comum V0_003). */
@Repository
public class OutboxStore {

    public record PendingEvent(UUID id, String eventType, String messageKey, @Nullable UUID orderId,
                               String correlationId, @Nullable String causationId, int schemaVersion, String payload) {
    }

    private final JdbcClient jdbc;

    public OutboxStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(PendingEvent e) {
        jdbc.sql("""
                INSERT INTO outbox (id, event_type, message_key, order_id, correlation_id, causation_id, schema_version, payload)
                VALUES (:id, :type, :key, :orderId, :corr, :cause, :version, CAST(:payload AS jsonb))
                """)
                .param("id", e.id()).param("type", e.eventType()).param("key", e.messageKey())
                .param("orderId", e.orderId()).param("corr", e.correlationId()).param("cause", e.causationId())
                .param("version", e.schemaVersion()).param("payload", e.payload())
                .update();
    }

    /** Lote pendente em ordem de id; SKIP LOCKED permite mais de uma instância sem trabalho duplicado. */
    public List<PendingEvent> lockPending(int limit) {
        return jdbc.sql("""
                SELECT id, event_type, message_key, order_id, correlation_id, causation_id, schema_version, payload::text AS payload
                FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED
                """)
                .param("limit", limit)
                .query((rs, i) -> new PendingEvent(rs.getObject("id", UUID.class), rs.getString("event_type"),
                        rs.getString("message_key"), rs.getObject("order_id", UUID.class), rs.getString("correlation_id"),
                        rs.getString("causation_id"), rs.getInt("schema_version"), rs.getString("payload")))
                .list();
    }

    public void markPublished(UUID id, OffsetDateTime at) {
        jdbc.sql("UPDATE outbox SET published_at = :at, attempts = attempts + 1, last_error = NULL WHERE id = :id")
                .param("id", id).param("at", at).update();
    }

    public void markFailed(UUID id, String error) {
        jdbc.sql("UPDATE outbox SET attempts = attempts + 1, last_error = :error WHERE id = :id")
                .param("id", id).param("error", error).update();
    }

    public long countPending() {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE published_at IS NULL").query(Long.class).single();
    }
}
