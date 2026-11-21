package com.sentinel.reservation.payment;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class PaymentRepository {

    public record Payment(UUID id, UUID reservationId, String status, BigDecimal amount, String currency,
                          String providerRef, int attempts) {}

    private final JdbcClient jdbc;

    public PaymentRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Creates the payment row on first attempt, counts attempts on retries. Never regresses a final status. */
    public void recordAttempt(UUID reservationId, BigDecimal amount, String currency) {
        jdbc.sql("""
                INSERT INTO payments (id, reservation_id, status, amount, currency, attempts)
                VALUES (:id, :rid, 'PENDING', :amt, :cur, 1)
                ON CONFLICT (reservation_id) DO UPDATE SET attempts = payments.attempts + 1, updated_at = now()
                """)
                .param("id", UUID.randomUUID()).param("rid", reservationId).param("amt", amount).param("cur", currency)
                .update();
    }

    /**
     * Applies a status change only if it is a legal transition, so a late or duplicate writer (e.g. a
     * Kafka retry still in flight after the reconciler resolved the payment) cannot regress it:
     * <pre>
     *   PENDING|UNKNOWN → UNKNOWN | CAPTURED | DECLINED | VOIDED
     *   CAPTURED        → REFUNDED
     * </pre>
     * @return true if the row changed
     */
    public boolean setStatus(UUID reservationId, String status, String providerRef, String error) {
        return jdbc.sql("""
                UPDATE payments SET status = :st, provider_ref = coalesce(:ref, provider_ref),
                                    last_error = :err, updated_at = now()
                WHERE reservation_id = :rid
                  AND ((:st IN ('UNKNOWN', 'CAPTURED', 'DECLINED', 'VOIDED') AND status IN ('PENDING', 'UNKNOWN'))
                    OR (:st = 'REFUNDED' AND status = 'CAPTURED'))
                """)
                .param("st", status).param("ref", providerRef).param("err", error).param("rid", reservationId)
                .update() == 1;
    }

    public Optional<Payment> find(UUID reservationId) {
        return jdbc.sql("SELECT id, reservation_id, status, amount, currency, provider_ref, attempts FROM payments WHERE reservation_id = :rid")
                .param("rid", reservationId)
                .query((rs, i) -> new Payment(rs.getObject("id", UUID.class), rs.getObject("reservation_id", UUID.class),
                        rs.getString("status"), rs.getBigDecimal("amount"), rs.getString("currency"),
                        rs.getString("provider_ref"), rs.getInt("attempts")))
                .optional();
    }

    /** Money taken for a reservation that did not end up confirmed: must be refunded. */
    public List<UUID> capturedButNotConfirmed() {
        return jdbc.sql("""
                SELECT p.reservation_id FROM payments p JOIN reservations r ON r.id = p.reservation_id
                WHERE p.status = 'CAPTURED' AND r.status IN ('CANCELLED', 'EXPIRED', 'PAYMENT_FAILED')
                LIMIT 100
                """).query((rs, i) -> rs.getObject("reservation_id", UUID.class)).list();
    }

    /** Payments whose outcome we do not know (timeout) and that have been quiet for a while. */
    public List<UUID> unresolvedBefore(Instant before) {
        return jdbc.sql("""
                SELECT reservation_id FROM payments
                WHERE status IN ('PENDING', 'UNKNOWN') AND updated_at < :before
                ORDER BY updated_at LIMIT 100
                """).param("before", Timestamp.from(before)).query((rs, i) -> rs.getObject("reservation_id", UUID.class)).list();
    }

    /** @return true the first time this consumer sees this event id (in the caller's transaction). */
    public boolean markProcessed(String consumer, UUID eventId) {
        return jdbc.sql("INSERT INTO processed_events (consumer, event_id) VALUES (:c, :e) ON CONFLICT DO NOTHING")
                .param("c", consumer).param("e", eventId).update() == 1;
    }

    public boolean alreadyProcessed(String consumer, UUID eventId) {
        return jdbc.sql("SELECT EXISTS (SELECT 1 FROM processed_events WHERE consumer = :c AND event_id = :e)")
                .param("c", consumer).param("e", eventId).query(Boolean.class).single();
    }
}
