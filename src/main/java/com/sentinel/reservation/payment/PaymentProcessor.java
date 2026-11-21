package com.sentinel.reservation.payment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinel.reservation.common.ReservationEvent;
import com.sentinel.reservation.reservation.Reservation;
import com.sentinel.reservation.reservation.ReservationRepository;
import com.sentinel.reservation.reservation.ReservationService;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;

/**
 * The payment step of the reservation saga (orchestrated, single service).
 *
 * <pre>
 *  RESERVATION_HELD      → charge(key = "charge:{id}") → CAPTURED: confirm      | DECLINED: compensate (release)
 *                                                       timeout: mark UNKNOWN, rethrow → Kafka retry (same key)
 *  CANCELLED / EXPIRED /
 *  PAYMENT_FAILED        → refund if money was captured (compensation)
 * </pre>
 *
 * Why the provider call is outside any DB transaction: holding row locks across a network call to a
 * third party would let provider latency block inventory for everyone. Instead each DB step is a short
 * transaction and correctness comes from idempotency (provider key + processed_events + state checks).
 */
@Component
public class PaymentProcessor {

    static final String CONSUMER = "payments";
    private static final Logger log = LoggerFactory.getLogger(PaymentProcessor.class);

    private final PaymentGateway gateway;
    private final PaymentRepository payments;
    private final ReservationRepository reservations;
    private final ReservationService reservationService;
    private final TransactionTemplate tx;
    private final ObjectMapper mapper;
    private final MeterRegistry metrics;

    public PaymentProcessor(PaymentGateway gateway, PaymentRepository payments, ReservationRepository reservations,
                            ReservationService reservationService, TransactionTemplate tx, ObjectMapper mapper,
                            MeterRegistry metrics) {
        this.gateway = gateway;
        this.payments = payments;
        this.reservations = reservations;
        this.reservationService = reservationService;
        this.tx = tx;
        this.mapper = mapper;
        this.metrics = metrics;
    }

    @KafkaListener(id = "sentinel-payments", topics = "${sentinel.reservation-topic}", groupId = "sentinel-payments")
    public void onMessage(ConsumerRecord<String, String> record) throws JsonProcessingException {
        handle(mapper.readValue(record.value(), ReservationEvent.class));
    }

    public void handle(ReservationEvent event) {
        if (payments.alreadyProcessed(CONSUMER, event.eventId())) {
            metrics.counter("sentinel.events.duplicate", "consumer", CONSUMER).increment();
            return;
        }
        switch (event.type()) {
            case RESERVATION_HELD -> charge(event);
            case RESERVATION_CANCELLED, RESERVATION_EXPIRED, RESERVATION_PAYMENT_FAILED -> {
                refundIfCaptured(event.reservationId());
                tx.executeWithoutResult(s -> payments.markProcessed(CONSUMER, event.eventId()));
            }
            case RESERVATION_CONFIRMED -> tx.executeWithoutResult(s -> payments.markProcessed(CONSUMER, event.eventId()));
        }
    }

    private void charge(ReservationEvent event) {
        UUID id = event.reservationId();
        Reservation r = reservations.find(id).orElseThrow();
        if (r.status() != Reservation.Status.PENDING_PAYMENT) {
            // Cancelled or expired before we got to it: do not charge.
            tx.executeWithoutResult(s -> payments.markProcessed(CONSUMER, event.eventId()));
            return;
        }
        tx.executeWithoutResult(s -> payments.recordAttempt(id, r.amount(), r.currency()));

        PaymentGateway.ChargeResult result;
        try {
            result = gateway.charge("charge:" + id, r.amount(), r.currency(), r.paymentToken());
        } catch (PaymentGateway.PaymentTimeoutException e) {
            tx.executeWithoutResult(s -> payments.setStatus(id, "UNKNOWN", null, e.getMessage()));
            metrics.counter("sentinel.payment.outcome", "result", "timeout").increment();
            throw e; // Kafka error handler retries with backoff; the same key makes the retry safe
        }
        applyChargeResult(id, result, event.eventId());
    }

    /** Applies a known provider outcome. Shared by the consumer path and the reconciler. */
    void applyChargeResult(UUID id, PaymentGateway.ChargeResult result, UUID eventIdOrNull) {
        Boolean refundNeeded = tx.execute(s -> {
            if (eventIdOrNull != null) {
                payments.markProcessed(CONSUMER, eventIdOrNull);
            }
            if (result.outcome() == PaymentGateway.Outcome.CAPTURED) {
                payments.setStatus(id, "CAPTURED", result.providerRef(), null);
                var applied = reservationService.onPaymentCaptured(id);
                return applied == ReservationService.PaymentApplyResult.ALREADY_FINAL_REFUND_NEEDED;
            }
            payments.setStatus(id, "DECLINED", result.providerRef(), result.declineReason());
            reservationService.onPaymentDeclined(id, "payment declined: " + result.declineReason());
            return false;
        });
        metrics.counter("sentinel.payment.outcome", "result", result.outcome().name().toLowerCase()).increment();
        if (Boolean.TRUE.equals(refundNeeded)) {
            log.info("Reservation {} was no longer pending when payment captured; refunding", id);
            refundIfCaptured(id);
        }
    }

    void voidIfReservationTerminal(UUID id) {
        tx.executeWithoutResult(s -> reservations.lock(id)
                .filter(r -> r.status().isTerminal())
                .ifPresent(r -> payments.setStatus(id, "VOIDED", null, "no charge found at provider")));
    }

    /** Compensation. Idempotent: provider key "refund:{id}" and a status check. */
    void refundIfCaptured(UUID id) {
        payments.find(id).filter(p -> "CAPTURED".equals(p.status())).ifPresent(p -> {
            String ref = gateway.refund("refund:" + id, p.providerRef(), p.amount());
            tx.executeWithoutResult(s -> payments.setStatus(id, "REFUNDED", ref, null));
            metrics.counter("sentinel.payment.outcome", "result", "refunded").increment();
        });
    }
}
