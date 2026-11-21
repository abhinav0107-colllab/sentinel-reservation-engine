package com.sentinel.reservation.inventory;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * All inventory SQL. Statements used in the reservation path are mirrored verbatim in
 * tools/sql_concurrency_check.py, which verifies them under concurrency against real PostgreSQL.
 */
@Repository
public class InventoryRepository {

    public record Night(LocalDate stayDate, int total, int reserved, BigDecimal price, String currency) {
        public int available() {
            return total - reserved;
        }
    }

    public record RoomType(UUID id, UUID hotelId, String name, int maxGuests) {}

    private final JdbcClient jdbc;

    public InventoryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Locks every night of the stay, <b>always in stay_date order</b>. Two overlapping multi-night
     * bookings therefore acquire their common rows in the same order and cannot deadlock: the second
     * one simply waits for the first to commit. Must run inside a transaction.
     */
    public List<Night> lockNights(UUID roomTypeId, LocalDate checkIn, LocalDate checkOut) {
        return jdbc.sql("""
                SELECT stay_date, total, reserved, price, currency FROM room_inventory
                WHERE room_type_id = :rt AND stay_date >= :ci AND stay_date < :co
                ORDER BY stay_date FOR UPDATE
                """)
                .param("rt", roomTypeId).param("ci", Date.valueOf(checkIn)).param("co", Date.valueOf(checkOut))
                .query((rs, i) -> new Night(rs.getDate("stay_date").toLocalDate(), rs.getInt("total"),
                        rs.getInt("reserved"), rs.getBigDecimal("price"), rs.getString("currency")))
                .list();
    }

    /** Caller must hold the row locks from {@link #lockNights}. The CHECK constraint is the backstop. */
    public int take(UUID roomTypeId, LocalDate checkIn, LocalDate checkOut, int rooms) {
        return jdbc.sql("""
                UPDATE room_inventory SET reserved = reserved + :rooms, updated_at = now()
                WHERE room_type_id = :rt AND stay_date >= :ci AND stay_date < :co
                """)
                .param("rooms", rooms).param("rt", roomTypeId)
                .param("ci", Date.valueOf(checkIn)).param("co", Date.valueOf(checkOut))
                .update();
    }

    public int giveBack(UUID roomTypeId, LocalDate checkIn, LocalDate checkOut, int rooms) {
        return jdbc.sql("""
                UPDATE room_inventory SET reserved = reserved - :rooms, updated_at = now()
                WHERE room_type_id = :rt AND stay_date >= :ci AND stay_date < :co
                """)
                .param("rooms", rooms).param("rt", roomTypeId)
                .param("ci", Date.valueOf(checkIn)).param("co", Date.valueOf(checkOut))
                .update();
    }

    /** Read-only availability (no locks). May be stale by the time the user books; booking re-checks under lock. */
    public List<Night> nights(UUID roomTypeId, LocalDate checkIn, LocalDate checkOut) {
        return jdbc.sql("""
                SELECT stay_date, total, reserved, price, currency FROM room_inventory
                WHERE room_type_id = :rt AND stay_date >= :ci AND stay_date < :co ORDER BY stay_date
                """)
                .param("rt", roomTypeId).param("ci", Date.valueOf(checkIn)).param("co", Date.valueOf(checkOut))
                .query((rs, i) -> new Night(rs.getDate("stay_date").toLocalDate(), rs.getInt("total"),
                        rs.getInt("reserved"), rs.getBigDecimal("price"), rs.getString("currency")))
                .list();
    }

    public void insertHotel(UUID id, String name) {
        jdbc.sql("INSERT INTO hotels (id, name) VALUES (:id, :name)").param("id", id).param("name", name).update();
    }

    public void insertRoomType(UUID id, UUID hotelId, String name, int maxGuests) {
        jdbc.sql("INSERT INTO room_types (id, hotel_id, name, max_guests) VALUES (:id, :h, :n, :g)")
                .param("id", id).param("h", hotelId).param("n", name).param("g", maxGuests).update();
    }

    public Optional<RoomType> findRoomType(UUID id) {
        return jdbc.sql("SELECT id, hotel_id, name, max_guests FROM room_types WHERE id = :id")
                .param("id", id)
                .query((rs, i) -> new RoomType(rs.getObject("id", UUID.class), rs.getObject("hotel_id", UUID.class),
                        rs.getString("name"), rs.getInt("max_guests")))
                .optional();
    }

    /**
     * Sets total and price for each night in [from, to). Lowering total below what is already reserved
     * is rejected by the CHECK constraint (the hotel must cancel bookings first), which the caller maps
     * to 409.
     */
    public int upsertNights(UUID roomTypeId, LocalDate from, LocalDate to, int total, BigDecimal price, String currency) {
        return jdbc.sql("""
                INSERT INTO room_inventory (room_type_id, stay_date, total, price, currency)
                SELECT :rt, d::date, :total, :price, :cur
                FROM generate_series(CAST(:from AS date), CAST(:to AS date) - 1, interval '1 day') AS d
                ON CONFLICT (room_type_id, stay_date) DO UPDATE
                   SET total = EXCLUDED.total, price = EXCLUDED.price, currency = EXCLUDED.currency, updated_at = now()
                """)
                .param("rt", roomTypeId).param("from", Date.valueOf(from)).param("to", Date.valueOf(to))
                .param("total", total).param("price", price).param("cur", currency)
                .update();
    }

    public record Drift(UUID roomTypeId, LocalDate stayDate, int total, int reserved, int heldByReservations) {}

    /**
     * Invariant audit: for every night, {@code reserved} must equal the rooms of reservations that
     * currently hold inventory, and must not exceed {@code total}. Returns violating rows (expected: none).
     */
    public List<Drift> audit() {
        return jdbc.sql("""
                SELECT i.room_type_id, i.stay_date, i.total, i.reserved, coalesce(sum(r.rooms), 0) AS held
                FROM room_inventory i
                LEFT JOIN reservations r ON r.room_type_id = i.room_type_id AND r.holds_inventory
                                        AND i.stay_date >= r.check_in AND i.stay_date < r.check_out
                GROUP BY i.room_type_id, i.stay_date, i.total, i.reserved
                HAVING i.reserved <> coalesce(sum(r.rooms), 0) OR i.reserved > i.total
                ORDER BY i.room_type_id, i.stay_date
                """)
                .query((rs, i) -> new Drift(rs.getObject("room_type_id", UUID.class), rs.getDate("stay_date").toLocalDate(),
                        rs.getInt("total"), rs.getInt("reserved"), rs.getInt("held")))
                .list();
    }
}
