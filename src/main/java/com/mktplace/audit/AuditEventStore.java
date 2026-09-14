package com.mktplace.audit;

import com.mktplace.commons.audit.AuditRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tabela audit_events (V50): uma linha por AuditRecord, idempotente pelo
 * recordId.
 */
@Repository
@Profile("audit")
public class AuditEventStore {

    public record Row(UUID id, String kind, String service, String eventId, String eventType, String topic,
            Integer partition, Long offset, String key, UUID orderId, String correlationId, String causationId,
            Integer schemaVersion, String producer, OffsetDateTime producedAt, Map<String, String> headers,
            String payload, String consumerGroup, OffsetDateTime consumedAt, Long processingTimeMs,
            Integer attempt, String status, String errorClass, String errorMessage, OffsetDateTime recordedAt) {
    }

    static final String COLUMNS = """
            id, kind, service, event_id, event_type, topic, partition, kafka_offset, message_key, order_id, correlation_id,
            causation_id, schema_version, producer, produced_at, headers::text AS headers, payload_raw, consumer_group,
            consumed_at, processing_time_ms, attempt, status, error_class, error_message, recorded_at
            """;

    private final JdbcClient jdbc;
    private final JsonMapper json;
    private final RowMapper<Row> mapper;

    public AuditEventStore(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
        this.mapper = (rs, i) -> new Row(rs.getObject("id", UUID.class), rs.getString("kind"), rs.getString("service"),
                rs.getString("event_id"), rs.getString("event_type"), rs.getString("topic"),
                (Integer) rs.getObject("partition"), (Long) rs.getObject("kafka_offset"), rs.getString("message_key"),
                rs.getObject("order_id", UUID.class), rs.getString("correlation_id"), rs.getString("causation_id"),
                (Integer) rs.getObject("schema_version"), rs.getString("producer"),
                rs.getObject("produced_at", OffsetDateTime.class), json.readValue(rs.getString("headers"), Map.class),
                rs.getString("payload_raw"), rs.getString("consumer_group"),
                rs.getObject("consumed_at", OffsetDateTime.class),
                (Long) rs.getObject("processing_time_ms"), (Integer) rs.getObject("attempt"), rs.getString("status"),
                rs.getString("error_class"), rs.getString("error_message"),
                rs.getObject("recorded_at", OffsetDateTime.class));
    }

    public boolean insert(AuditRecord r) {
        String payloadJson = null;
        if (r.payload() != null) {
            try {
                payloadJson = json.writeValueAsString(json.readTree(r.payload()));
            } catch (RuntimeException ignored) {
                // payload não é JSON: fica só em payload_raw
            }
        }
        UUID orderId = null;
        try {
            orderId = r.orderId() == null ? null : UUID.fromString(r.orderId());
        } catch (IllegalArgumentException ignored) {
            // chave que não é pedido (ex.: sku de StockAdjusted)
        }
        return jdbc
                .sql("""
                        INSERT INTO audit_events (id, kind, service, event_id, event_type, topic, partition, kafka_offset, message_key,
                            order_id, correlation_id, causation_id, schema_version, producer, produced_at, headers, payload, payload_raw,
                            consumer_group, consumed_at, processing_time_ms, attempt, status, error_class, error_message, recorded_at)
                        VALUES (:id, :kind, :service, :eventId, :eventType, :topic, :partition, :offset, :key, :orderId, :corr, :cause,
                            :schema, :producer, :producedAt, CAST(:headers AS jsonb), CAST(:payload AS jsonb), :payloadRaw, :group,
                            :consumedAt, :ms, :attempt, :status, :errorClass, :errorMessage, :recordedAt)
                        ON CONFLICT (id) DO NOTHING
                        """)
                .param("id", r.recordId()).param("kind", r.kind().name()).param("service", r.service())
                .param("eventId", r.eventId()).param("eventType", r.eventType()).param("topic", r.topic())
                .param("partition", r.partition()).param("offset", r.offset()).param("key", r.key())
                .param("orderId", orderId)
                .param("corr", r.correlationId()).param("cause", r.causationId()).param("schema", r.schemaVersion())
                .param("producer", r.producer()).param("producedAt", r.producedAt())
                .param("headers", json.writeValueAsString(r.headers())).param("payload", payloadJson)
                .param("payloadRaw", r.payload()).param("group", r.consumerGroup()).param("consumedAt", r.consumedAt())
                .param("ms", r.processingTimeMs()).param("attempt", r.attempt())
                .param("status", r.status() == null ? null : r.status().name()).param("errorClass", r.errorClass())
                .param("errorMessage", r.errorMessage()).param("recordedAt", r.recordedAt())
                .update() == 1;
    }

    /** A linha do tempo inteira de um pedido em UMA query, na ordem de produção. */
    public List<Row> findByOrder(UUID orderId) {
        return jdbc.sql("SELECT " + COLUMNS
                + " FROM audit_events WHERE order_id = :orderId ORDER BY produced_at, event_id, kind, attempt, recorded_at")
                .param("orderId", orderId).query(mapper).list();
    }
}
