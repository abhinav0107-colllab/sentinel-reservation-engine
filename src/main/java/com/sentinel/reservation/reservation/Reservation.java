package com.sentinel.reservation.reservation;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

public record Reservation(
        UUID id,
        UUID hotelId,
        UUID roomTypeId,
        LocalDate checkIn,
        LocalDate checkOut,
        int rooms,
        String guestName,
        String guestEmail,
        BigDecimal amount,
        String currency,
        String paymentToken,
        Status status,
        boolean holdsInventory,
        Instant holdExpiresAt,
        String failureReason,
        long version,
        Instant createdAt) {

    /**
     * State machine. Only these transitions exist:
     * <pre>
     *   PENDING_PAYMENT ──payment captured──▶ CONFIRMED ──cancel──▶ CANCELLED
     *         │ ├──payment declined──▶ PAYMENT_FAILED
     *         │ ├──hold expired─────▶ EXPIRED
     *         └─┴──cancel───────────▶ CANCELLED
     * </pre>
     * Every transition out of PENDING_PAYMENT/CONFIRMED into a terminal state releases inventory.
     */
    public enum Status {
        PENDING_PAYMENT, CONFIRMED, CANCELLED, EXPIRED, PAYMENT_FAILED;

        public boolean isTerminal() {
            return this == CANCELLED || this == EXPIRED || this == PAYMENT_FAILED;
        }
    }
}
