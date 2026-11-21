package com.sentinel.reservation.reservation;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ReservationRepository {

    private static final String COLS = """
            id, hotel_id, room_type_id, check_in, check_out, rooms, guest_name, guest_email, amount, currency,
            payment_token, status, holds_inventory, hold_expires_at, failure_reason, version, created_at
            """;

    private final JdbcClient jdbc;

    public ReservationRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(Reservation r) {
        jdbc.sql("""
                INSERT INTO reservations (id, hotel_id, room_type_id, check_in, check_out, rooms, guest_name, guest_email,
                                          amount, currency, payment_token, status, hold_expires_at)
                VALUES (:id, :hotel, :rt, :ci, :co, :rooms, :gn, :ge, :amount, :cur, :tok, :status, :exp)
                """)
                .param("id", r.id()).param("hotel", r.hotelId()).param("rt", r.roomTypeId())
                .param("ci", Date.valueOf(r.checkIn())).param("co", Date.valueOf(r.checkOut()))
                .param("rooms", r.rooms()).param("gn", r.guestName()).param("ge", r.guestEmail())
                .param("amount", r.amount()).param("cur", r.currency()).param("tok", r.paymentToken())
                .param("status", r.status().name()).param("exp", Timestamp.from(r.holdExpiresAt()))
                .update();
    }

    public Optional<Reservation> find(UUID id) {
        return jdbc.sql("SELECT " + COLS + " FROM reservations WHERE id = :id").param("id", id).query(MAPPER).optional();
    }

    /** Row lock: serializes every state transition of one reservation (payment vs. expiry vs. cancel). */
    public Optional<Reservation> lock(UUID id) {
        return jdbc.sql("SELECT " + COLS + " FROM reservations WHERE id = :id FOR UPDATE").param("id", id).query(MAPPER).optional();
    }

    /** PENDING_PAYMENT -> CONFIRMED. Caller holds the row lock and has checked the current status. */
    public void confirm(UUID id) {
        jdbc.sql("UPDATE reservations SET status = 'CONFIRMED', version = version + 1, updated_at = now() WHERE id = :id")
                .param("id", id).update();
    }

    /**
     * Moves to a terminal status and gives up the inventory claim in one statement. Returns true only for
     * the single caller that actually flipped holds_inventory, so inventory is returned exactly once even
     * if cancel, expiry and payment failure race each other.
     */
    public boolean releaseTo(UUID id, Reservation.Status terminal, String reason) {
        return jdbc.sql("""
                UPDATE reservations SET status = :st, holds_inventory = FALSE, failure_reason = :reason,
                       version = version + 1, updated_at = now()
                WHERE id = :id AND holds_inventory
                """)
                .param("st", terminal.name()).param("reason", reason).param("id", id)
                .update() == 1;
    }

    /** Expired holds, oldest first; SKIP LOCKED lets several sweepers (instances) share the work. */
    public List<UUID> lockExpiredHolds(Instant now, int limit) {
        return jdbc.sql("""
                SELECT id FROM reservations
                WHERE status = 'PENDING_PAYMENT' AND hold_expires_at < :now
                ORDER BY hold_expires_at LIMIT :limit
                FOR UPDATE SKIP LOCKED
                """)
                .param("now", Timestamp.from(now)).param("limit", limit)
                .query((rs, i) -> rs.getObject("id", UUID.class)).list();
    }

    static final RowMapper<Reservation> MAPPER = (rs, i) -> new Reservation(
            rs.getObject("id", UUID.class),
            rs.getObject("hotel_id", UUID.class),
            rs.getObject("room_type_id", UUID.class),
            rs.getDate("check_in").toLocalDate(),
            rs.getDate("check_out").toLocalDate(),
            rs.getInt("rooms"),
            rs.getString("guest_name"),
            rs.getString("guest_email"),
            rs.getBigDecimal("amount"),
            rs.getString("currency"),
            rs.getString("payment_token"),
            Reservation.Status.valueOf(rs.getString("status")),
            rs.getBoolean("holds_inventory"),
            rs.getTimestamp("hold_expires_at").toInstant(),
            rs.getString("failure_reason"),
            rs.getLong("version"),
            rs.getTimestamp("created_at").toInstant());
}
