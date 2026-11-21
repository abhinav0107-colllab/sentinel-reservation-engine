package com.sentinel.reservation.payment;

import com.sentinel.reservation.common.ReservationEvent;
import com.sentinel.reservation.reservation.HoldExpirySweeper;
import com.sentinel.reservation.support.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Saga + failure injection: each payment token drives one failure mode of the simulated provider.
 * Flow under test: REST -> PostgreSQL (+outbox) -> relay -> Kafka -> PaymentProcessor -> provider -> PostgreSQL.
 */
class PaymentSagaIT extends IntegrationTestBase {

    private static final LocalDate IN = LocalDate.now().plusDays(50);
    private static final Duration WAIT = Duration.ofSeconds(30);

    @Autowired HoldExpirySweeper sweeper;
    @Autowired PaymentReconciler reconciler;
    @Autowired PaymentProcessor processor;

    private UUID book(UUID rt, String token) throws Exception {
        var resp = reserve(UUID.randomUUID().toString(), rt, IN, IN.plusDays(2), 1, token);
        assertThat(resp.getStatusCode().value()).isEqualTo(201);
        var body = json(resp.getBody());
        assertThat(body.path("status").asText()).isEqualTo("PENDING_PAYMENT");
        assertThat(body.path("amount").decimalValue()).isEqualByComparingTo("300.00"); // 2 nights x 150
        return UUID.fromString(body.path("id").asText());
    }

    private String paymentStatus(UUID id) {
        return jdbc.queryForList("SELECT status FROM payments WHERE reservation_id = ?", String.class, id)
                .stream().findFirst().orElse(null);
    }

    private int providerCharges(UUID id) {
        return jdbc.queryForObject("SELECT count(*) FROM simulated_provider_ledger WHERE idem_key = ?", Integer.class, "charge:" + id);
    }

    @Test
    void happyPath_paymentCapturedConfirmsReservation() throws Exception {
        UUID rt = roomTypeWithInventory(3, IN, IN.plusDays(2), "150.00");
        UUID id = book(rt, "tok_ok");
        await().atMost(WAIT).until(() -> "CONFIRMED".equals(status(id)));
        assertThat(paymentStatus(id)).isEqualTo("CAPTURED");
        assertThat(reserved(rt, IN)).isEqualTo(1);
        // Event trail in the outbox: held then confirmed
        assertThat(jdbc.queryForList("SELECT event_type FROM outbox_events WHERE aggregate_id = ? ORDER BY id", String.class, id))
                .containsExactly("RESERVATION_HELD", "RESERVATION_CONFIRMED");
    }

    @Test
    void declinedPayment_compensatesByReleasingInventory() throws Exception {
        UUID rt = roomTypeWithInventory(3, IN, IN.plusDays(2), "150.00");
        UUID id = book(rt, "tok_decline");
        await().atMost(WAIT).until(() -> "PAYMENT_FAILED".equals(status(id)));
        assertThat(paymentStatus(id)).isEqualTo("DECLINED");
        assertThat(reserved(rt, IN)).isZero();
        assertThat(inventoryConsistent()).isTrue();
    }

    @Test
    void ambiguousTimeoutAfterCapture_retryWithSameKeyConfirmsWithoutDoubleCharge() throws Exception {
        UUID rt = roomTypeWithInventory(3, IN, IN.plusDays(2), "150.00");
        UUID id = book(rt, "tok_timeout_after_capture");
        await().atMost(WAIT).until(() -> "CONFIRMED".equals(status(id)));
        assertThat(paymentStatus(id)).isEqualTo("CAPTURED");
        assertThat(providerCharges(id)).isEqualTo(1);
        Integer attempts = jdbc.queryForObject("SELECT attempts FROM payments WHERE reservation_id = ?", Integer.class, id);
        assertThat(attempts).isGreaterThanOrEqualTo(2);
    }

    @Test
    void providerOutage_holdExpiresInventoryReleasedAndPaymentVoided() throws Exception {
        UUID rt = roomTypeWithInventory(3, IN, IN.plusDays(2), "150.00");
        UUID id = book(rt, "tok_provider_down");
        await().atMost(WAIT).until(() -> "UNKNOWN".equals(paymentStatus(id)));
        assertThat(status(id)).isEqualTo("PENDING_PAYMENT");

        // Time passes beyond the hold TTL (10 min): the sweeper releases the inventory.
        assertThat(sweeper.sweepOnce(Instant.now().plus(Duration.ofMinutes(11)))).isEqualTo(1);
        assertThat(status(id)).isEqualTo("EXPIRED");
        assertThat(reserved(rt, IN)).isZero();

        // Reconciliation: provider has no charge, reservation is terminal -> payment closed out.
        reconciler.reconcileOnce(Instant.now().plus(Duration.ofMinutes(5)));
        assertThat(paymentStatus(id)).isEqualTo("VOIDED");
        assertThat(providerCharges(id)).isZero();
        assertThat(inventoryConsistent()).isTrue();
    }

    @Test
    void cancelAfterConfirmation_refundsAndReleases() throws Exception {
        UUID rt = roomTypeWithInventory(3, IN, IN.plusDays(2), "150.00");
        UUID id = book(rt, "tok_ok");
        await().atMost(WAIT).until(() -> "CONFIRMED".equals(status(id)));

        var cancel = rest.exchange("/api/v1/reservations/" + id + "/cancel", HttpMethod.POST, HttpEntity.EMPTY, String.class);
        assertThat(json(cancel.getBody()).path("status").asText()).isEqualTo("CANCELLED");
        assertThat(reserved(rt, IN)).isZero();
        await().atMost(WAIT).until(() -> "REFUNDED".equals(paymentStatus(id)));

        // Cancelling again is idempotent
        var again = rest.exchange("/api/v1/reservations/" + id + "/cancel", HttpMethod.POST, HttpEntity.EMPTY, String.class);
        assertThat(again.getStatusCode().value()).isEqualTo(200);
        assertThat(reserved(rt, IN)).isZero();
    }

    @Test
    void lateCaptureAfterExpiry_isRefunded() throws Exception {
        UUID rt = roomTypeWithInventory(3, IN, IN.plusDays(2), "150.00");
        UUID id = book(rt, "tok_provider_down");
        await().atMost(WAIT).until(() -> "UNKNOWN".equals(paymentStatus(id)));
        sweeper.sweepOnce(Instant.now().plus(Duration.ofMinutes(11)));
        assertThat(status(id)).isEqualTo("EXPIRED");

        // The provider later reports that the charge did go through.
        processor.applyChargeResult(id, new PaymentGateway.ChargeResult(PaymentGateway.Outcome.CAPTURED, "ch_late", null), null);
        assertThat(status(id)).isEqualTo("EXPIRED");          // never resurrected
        assertThat(paymentStatus(id)).isEqualTo("REFUNDED");  // compensated
        assertThat(reserved(rt, IN)).isZero();
    }

    @Test
    void duplicateEventDeliveryIsProcessedOnce() throws Exception {
        UUID rt = roomTypeWithInventory(3, IN, IN.plusDays(2), "150.00");
        UUID id = book(rt, "tok_ok");
        await().atMost(WAIT).until(() -> "CONFIRMED".equals(status(id)));

        var held = mapper.readValue(jdbc.queryForObject(
                "SELECT payload::text FROM outbox_events WHERE aggregate_id = ? AND event_type = 'RESERVATION_HELD'",
                String.class, id), ReservationEvent.class);
        processor.handle(held); // redelivery
        processor.handle(held); // and again

        assertThat(providerCharges(id)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT attempts FROM payments WHERE reservation_id = ?", Integer.class, id)).isEqualTo(1);
        assertThat(status(id)).isEqualTo("CONFIRMED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'RESERVATION_CONFIRMED'",
                Integer.class, id)).isEqualTo(1);
    }
}
