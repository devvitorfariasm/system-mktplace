package com.mktplace.commons.consumer;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/** Tabela processed_events (migration comum V0_001). */
@Repository
public class ProcessedEventStore {

    private final JdbcClient jdbc;

    public ProcessedEventStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** @return true se este é o primeiro processamento de (evento, grupo); false se já foi processado. */
    public boolean markProcessed(UUID eventId, String consumerGroup, String topic) {
        return jdbc.sql("""
                INSERT INTO processed_events (event_id, consumer_group, topic)
                VALUES (:eventId, :group, :topic)
                ON CONFLICT DO NOTHING
                """)
                .param("eventId", eventId).param("group", consumerGroup).param("topic", topic)
                .update() == 1;
    }

    public boolean wasProcessed(UUID eventId, String consumerGroup) {
        return jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = :eventId AND consumer_group = :group")
                .param("eventId", eventId).param("group", consumerGroup)
                .query(Long.class).single() > 0;
    }
}
