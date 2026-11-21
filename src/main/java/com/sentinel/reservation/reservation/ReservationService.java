package com.sentinel.reservation.reservation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.reservation.common.ReservationEvent;
import com.sentinel.reservation.config.SentinelProperties;
import com.sentinel.reservation.inventory.InventoryRepository;
import com.sentinel.reservation.outbox.OutboxWriter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/**
 * Reservation state machine and inventory accounting.
 *
 * <p>Lock order, everywhere: reservation row first, then inventory rows in stay_date order. Creating a
 * hold locks only inventory rows (the reservation row is new), so no cycle exists between any two
 * code paths and deadlocks cannot occur. This was checked under contention (tools/sql_concurrency_check.py).
 */
@Service
public class ReservationService {

    public static final String OP_CREATE = "create_reservation";

    /** What the API returns; also what is stored for idempotent replay. */
    public record ReservationView(UUID id, String status, UUID hotelId, UUID roomTypeId, String checkIn, String checkOut,
                                  int rooms, BigDecimal amount, String currency, Instant holdExpiresAt, String failureReason) {
        static ReservationView of(Reservation r) {
            return new ReservationView(r.id(), r.status().name(), r.hotelId(), r.roomTypeId(), r.checkIn().toString(),
                    r.checkOut().toString(), r.rooms(), r.amount(), r.currency(), r.holdExpiresAt(), r.failureReason());
        }
    }

    public record CreateResult(int httpStatus, String body, boolean replayed) {}

    private final ReservationRepository reservations;
    private final InventoryRepository inventory;
    private final IdempotencyRepository idempotency;
    private final OutboxWriter outbox;
    private final ObjectMapper mapper;
    private final SentinelProperties props;
    private final MeterRegistry metrics;

    public ReservationService(ReservationRepository reservations, InventoryRepository inventory,
                              IdempotencyRepository idempotency, OutboxWriter outbox, ObjectMapper mapper,
                              SentinelProperties props, MeterRegistry metrics) {
        this.reservations = reservations;
        this.inventory = inventory;
        this.idempotency = idempotency;
        this.outbox = outbox;
        this.mapper = mapper;
        this.props = props;
        this.metrics = metrics;
    }

    /**
     * Creates a payment-pending hold. Idempotency claim, inventory decrement, reservation row, outbox
     * event and stored response all commit in ONE transaction, or none of them do.
     */
    @Transactional
    public CreateResult create(String idempotencyKey, CreateReservationRequest req) {
        validateDates(req);
        String hash = requestHash(req);
        if (!idempotency.claim(OP_CREATE, idempotencyKey, hash)) {
            var stored = idempotency.find(OP_CREATE, idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException("idempotency key vanished: " + idempotencyKey));
            if (!stored.requestHash().equals(hash)) {
                throw new ReservationExceptions.KeyReuse();
            }
            metrics.counter("sentinel.reservation.create", "outcome", "replayed").increment();
            return new CreateResult(stored.status(), stored.body(), true);
        }

        var roomType = inventory.findRoomType(req.roomTypeId())
                .orElseThrow(() -> new ReservationExceptions.NotFound("room type not found"));
        long nights = ChronoUnit.DAYS.between(req.checkIn(), req.checkOut());
        List<InventoryRepository.Night> locked = inventory.lockNights(req.roomTypeId(), req.checkIn(), req.checkOut());
        if (locked.size() != nights) {
            metrics.counter("sentinel.reservation.create", "outcome", "sold_out").increment();
            throw new ReservationExceptions.SoldOut("no inventory loaded for some of the requested nights");
        }
        for (var night : locked) {
            if (night.available() < req.rooms()) {
                metrics.counter("sentinel.reservation.create", "outcome", "sold_out").increment();
                throw new ReservationExceptions.SoldOut("only " + night.available() + " room(s) left on " + night.stayDate());
            }
        }
        inventory.take(req.roomTypeId(), req.checkIn(), req.checkOut(), req.rooms());

        BigDecimal amount = locked.stream().map(InventoryRepository.Night::price)
                .reduce(BigDecimal.ZERO, BigDecimal::add).multiply(BigDecimal.valueOf(req.rooms()));
        String currency = locked.get(0).currency();
        Instant now = Instant.now();
        var r = new Reservation(UUID.randomUUID(), roomType.hotelId(), req.roomTypeId(), req.checkIn(), req.checkOut(),
                req.rooms(), req.guestName(), req.guestEmail(), amount, currency, req.paymentToken(),
                Reservation.Status.PENDING_PAYMENT, true, now.plus(props.holdTtl()), null, 1, now);
        reservations.insert(r);
        emit(ReservationEvent.Type.RESERVATION_HELD, r, null);

        String body = json(ReservationView.of(r));
        idempotency.complete(OP_CREATE, idempotencyKey, 201, body);
        metrics.counter("sentinel.reservation.create", "outcome", "held").increment();
        return new CreateResult(201, body, false);
    }

    /** Cancel is naturally idempotent: cancelling a CANCELLED reservation returns it unchanged. */
    @Transactional
    public ReservationView cancel(UUID id) {
        Reservation r = reservations.lock(id).orElseThrow(() -> new ReservationExceptions.NotFound("reservation not found"));
        if (r.status() == Reservation.Status.CANCELLED) {
            return ReservationView.of(r);
        }
        if (r.status().isTerminal()) {
            throw new ReservationExceptions.InvalidState("cannot cancel a reservation in status " + r.status());
        }
        releaseLocked(r, Reservation.Status.CANCELLED, "cancelled by guest", ReservationEvent.Type.RESERVATION_CANCELLED);
        return ReservationView.of(reservations.find(id).orElseThrow());
    }

    @Transactional(readOnly = true)
    public ReservationView get(UUID id) {
        return reservations.find(id).map(ReservationView::of)
                .orElseThrow(() -> new ReservationExceptions.NotFound("reservation not found"));
    }

    // ---------------------------------------------------------------- saga steps (called by PaymentProcessor)

    public enum PaymentApplyResult { CONFIRMED, ALREADY_FINAL_REFUND_NEEDED, IGNORED }

    /**
     * Payment captured. Confirms if still pending; if the hold already expired or was cancelled, the
     * caller must refund (compensation). Must be called inside the caller's transaction.
     */
    @Transactional
    public PaymentApplyResult onPaymentCaptured(UUID id) {
        Reservation r = reservations.lock(id).orElseThrow(() -> new ReservationExceptions.NotFound("reservation " + id));
        if (r.status() == Reservation.Status.PENDING_PAYMENT) {
            reservations.confirm(id);
            emit(ReservationEvent.Type.RESERVATION_CONFIRMED, r, null);
            metrics.counter("sentinel.reservation.transition", "to", "CONFIRMED").increment();
            return PaymentApplyResult.CONFIRMED;
        }
        if (r.status() == Reservation.Status.CONFIRMED) {
            return PaymentApplyResult.IGNORED; // duplicate
        }
        return PaymentApplyResult.ALREADY_FINAL_REFUND_NEEDED;
    }

    /** Payment declined: compensate by releasing the inventory. No-op if already final. */
    @Transactional
    public void onPaymentDeclined(UUID id, String reason) {
        Reservation r = reservations.lock(id).orElseThrow(() -> new ReservationExceptions.NotFound("reservation " + id));
        if (r.status() == Reservation.Status.PENDING_PAYMENT) {
            releaseLocked(r, Reservation.Status.PAYMENT_FAILED, reason, ReservationEvent.Type.RESERVATION_PAYMENT_FAILED);
        }
    }

    /** Called by the expiry sweeper for a row it has already locked with SKIP LOCKED. */
    @Transactional
    public void expireLocked(UUID id) {
        Reservation r = reservations.lock(id).orElseThrow();
        if (r.status() == Reservation.Status.PENDING_PAYMENT) {
            releaseLocked(r, Reservation.Status.EXPIRED, "payment not completed before hold expiry",
                    ReservationEvent.Type.RESERVATION_EXPIRED);
        }
    }

    // ---------------------------------------------------------------- internals

    private void releaseLocked(Reservation r, Reservation.Status to, String reason, ReservationEvent.Type eventType) {
        if (reservations.releaseTo(r.id(), to, reason)) {
            inventory.lockNights(r.roomTypeId(), r.checkIn(), r.checkOut()); // same lock order as create
            inventory.giveBack(r.roomTypeId(), r.checkIn(), r.checkOut(), r.rooms());
        }
        emit(eventType, r, reason);
        metrics.counter("sentinel.reservation.transition", "to", to.name()).increment();
    }

    private void emit(ReservationEvent.Type type, Reservation r, String reason) {
        UUID eventId = UUID.randomUUID();
        String status = switch (type) {
            case RESERVATION_HELD -> "PENDING_PAYMENT";
            case RESERVATION_CONFIRMED -> "CONFIRMED";
            case RESERVATION_PAYMENT_FAILED -> "PAYMENT_FAILED";
            case RESERVATION_CANCELLED -> "CANCELLED";
            case RESERVATION_EXPIRED -> "EXPIRED";
        };
        var event = new ReservationEvent(eventId, ReservationEvent.SCHEMA_VERSION, type, Instant.now(), r.id(), status,
                r.hotelId(), r.roomTypeId(), r.checkIn(), r.checkOut(), r.rooms(), r.amount(), r.currency(), reason);
        outbox.append(eventId, "reservation", r.id(), type.name(), props.reservationTopic(), event);
    }

    private static void validateDates(CreateReservationRequest req) {
        long nights = ChronoUnit.DAYS.between(req.checkIn(), req.checkOut());
        if (nights < 1 || nights > 30) {
            throw new IllegalArgumentException("checkOut must be 1 to 30 nights after checkIn");
        }
    }

    String requestHash(CreateReservationRequest req) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(json(req).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(d);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private String json(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
