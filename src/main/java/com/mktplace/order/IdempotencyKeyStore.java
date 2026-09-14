package com.mktplace.order;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

/** Tabela idempotency_keys: resposta original de POST /orders por Idempotency-Key. */
@Repository
public class IdempotencyKeyStore {

    public record Stored(String requestHash, UUID orderId, String responseBody) {
    }

    private final JdbcClient jdbc;

    public IdempotencyKeyStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<Stored> find(String key) {
        return jdbc.sql("SELECT request_hash, order_id, response_body::text AS body FROM idempotency_keys WHERE idem_key = :key")
                .param("key", key)
                .query((rs, i) -> new Stored(rs.getString("request_hash"), rs.getObject("order_id", UUID.class), rs.getString("body")))
                .optional();
    }

    public void save(String key, String requestHash, UUID orderId, String responseBody) {
        jdbc.sql("""
                INSERT INTO idempotency_keys (idem_key, request_hash, order_id, response_body)
                VALUES (:key, :hash, :orderId, CAST(:body AS jsonb))
                """)
                .param("key", key).param("hash", requestHash).param("orderId", orderId).param("body", responseBody)
                .update();
    }
}
