package com.sentinel.reservation.common;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/**
 * Published to {@code sentinel.reservation-events.v1}, keyed by reservationId. At-least-once; consumers
 * deduplicate on {@code eventId} via the processed_events table.
 */
public record ReservationEvent(
        UUID eventId,
        int schemaVersion,
        Type type,
        Instant occurredAt,
        UUID reservationId,
        String status,
        UUID hotelId,
        UUID roomTypeId,
        LocalDate checkIn,
        LocalDate checkOut,
        int rooms,
        BigDecimal amount,
        String currency,
        String reason) {

    public static final int SCHEMA_VERSION = 1;

    public enum Type {
        RESERVATION_HELD,
        RESERVATION_CONFIRMED,
        RESERVATION_PAYMENT_FAILED,
        RESERVATION_CANCELLED,
        RESERVATION_EXPIRED
    }
}
