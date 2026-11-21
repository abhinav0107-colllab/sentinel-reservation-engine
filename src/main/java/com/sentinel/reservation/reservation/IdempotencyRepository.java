package com.sentinel.reservation.reservation;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Idempotency keys live in PostgreSQL, written in the same transaction as the business change.
 *
 * <p>Protocol (all inside the caller's transaction):
 * <ol>
 *   <li>{@link #claim}: INSERT ... ON CONFLICT DO NOTHING. If another transaction holds the same key
 *       uncommitted, PostgreSQL blocks this INSERT until that transaction ends, so duplicates are
 *       serialized by the unique index, not by application locks.</li>
 *   <li>Claimed → do the work → {@link #complete} with the response.</li>
 *   <li>Not claimed → {@link #find} returns the committed response, which is replayed.</li>
 * </ol>
 * Because claim and complete commit atomically with the reservation, a stored key always has a final
 * response. A failed attempt (e.g. sold out) rolls the claim back too, so the client may retry it.
 */
@Repository
public class IdempotencyRepository {

    public record Stored(String requestHash, int status, String body) {}

    private final JdbcClient jdbc;

    public IdempotencyRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public boolean claim(String operation, String key, String requestHash) {
        return jdbc.sql("""
                INSERT INTO idempotency_keys (idem_key, operation, request_hash, response_status, response_body)
                VALUES (:key, :op, :hash, 0, '{}'::jsonb) ON CONFLICT DO NOTHING
                """)
                .param("key", key).param("op", operation).param("hash", requestHash)
                .update() == 1;
    }

    public Optional<Stored> find(String operation, String key) {
        return jdbc.sql("SELECT request_hash, response_status, response_body::text AS body FROM idempotency_keys "
                        + "WHERE operation = :op AND idem_key = :key")
                .param("op", operation).param("key", key)
                .query((rs, i) -> new Stored(rs.getString("request_hash"), rs.getInt("response_status"), rs.getString("body")))
                .optional();
    }

    public void complete(String operation, String key, int status, String bodyJson) {
        jdbc.sql("UPDATE idempotency_keys SET response_status = :st, response_body = CAST(:body AS jsonb) "
                        + "WHERE operation = :op AND idem_key = :key")
                .param("st", status).param("body", bodyJson).param("op", operation).param("key", key)
                .update();
    }

    public int deleteOlderThanDays(int days) {
        return jdbc.sql("DELETE FROM idempotency_keys WHERE created_at < now() - make_interval(days => :d)")
                .param("d", days).update();
    }
}
