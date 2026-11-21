package com.sentinel.reservation.outbox;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

@Repository
public class OutboxRepository {

    public record OutboxRow(long id, UUID eventId, UUID aggregateId, String topic, String payload, int attempts) {}

    private final JdbcClient jdbc;

    public OutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Must be called inside the caller's business transaction (MANDATORY propagation is enforced by OutboxWriter). */
    public void insert(UUID eventId, String aggregateType, UUID aggregateId, String eventType, String topic, String payloadJson) {
        jdbc.sql("""
                INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, event_type, topic, payload)
                VALUES (:eventId, :aggregateType, :aggregateId, :eventType, :topic, CAST(:payload AS jsonb))
                """)
                .param("eventId", eventId)
                .param("aggregateType", aggregateType)
                .param("aggregateId", aggregateId)
                .param("eventType", eventType)
                .param("topic", topic)
                .param("payload", payloadJson)
                .update();
    }

    /**
     * Claims a batch of unpublished rows. {@code FOR UPDATE SKIP LOCKED} lets several application
     * instances run the relay concurrently without publishing the same row twice in parallel and
     * without blocking each other. Must run inside a transaction; the locks are held until commit.
     */
    public List<OutboxRow> lockNextBatch(int limit) {
        return jdbc.sql("""
                SELECT id, event_id, aggregate_id, topic, payload::text AS payload, attempts
                FROM outbox_events
                WHERE published_at IS NULL
                ORDER BY id
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
                """)
                .param("limit", limit)
                .query((rs, n) -> new OutboxRow(
                        rs.getLong("id"),
                        rs.getObject("event_id", UUID.class),
                        rs.getObject("aggregate_id", UUID.class),
                        rs.getString("topic"),
                        rs.getString("payload"),
                        rs.getInt("attempts")))
                .list();
    }

    public void markPublished(List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbc.sql("UPDATE outbox_events SET published_at = now(), attempts = attempts + 1 WHERE id IN (:ids)")
                .param("ids", ids)
                .update();
    }

    public void markFailed(long id, String error) {
        jdbc.sql("UPDATE outbox_events SET attempts = attempts + 1, last_error = :err WHERE id = :id")
                .param("id", id)
                .param("err", error == null ? "unknown" : error.substring(0, Math.min(error.length(), 1000)))
                .update();
    }

    public long countUnpublished() {
        return jdbc.sql("SELECT count(*) FROM outbox_events WHERE published_at IS NULL").query(Long.class).single();
    }

    /** Age of the oldest unpublished event: the end-to-end "publish lag" SLI. */
    public Duration oldestUnpublishedAge() {
        Timestamp ts = jdbc.sql("SELECT min(created_at) FROM outbox_events WHERE published_at IS NULL")
                .query(Timestamp.class).single();
        return ts == null ? Duration.ZERO : Duration.between(ts.toInstant(), Instant.now());
    }

    public int deletePublishedBefore(Instant cutoff) {
        return jdbc.sql("DELETE FROM outbox_events WHERE published_at IS NOT NULL AND published_at < :cutoff")
                .param("cutoff", Timestamp.from(cutoff))
                .update();
    }
}
