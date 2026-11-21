package com.sentinel.reservation.payment;

import com.sentinel.reservation.config.SentinelProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Failure recovery for the payment step, independent of Kafka:
 * <ol>
 *   <li>Unknown outcomes (timeouts whose retries were exhausted or whose consumer died): ask the provider
 *       by idempotency key and apply the real outcome.</li>
 *   <li>Money captured for a reservation that ended up cancelled/expired/failed (e.g. crash between
 *       "capture recorded" and "refund issued"): refund it.</li>
 * </ol>
 * Both steps are idempotent, so running them repeatedly or on several instances is safe.
 */
@Component
public class PaymentReconciler {

    private static final Logger log = LoggerFactory.getLogger(PaymentReconciler.class);

    private final PaymentRepository payments;
    private final PaymentGateway gateway;
    private final PaymentProcessor processor;
    private final SentinelProperties props;

    public PaymentReconciler(PaymentRepository payments, PaymentGateway gateway, PaymentProcessor processor,
                             SentinelProperties props) {
        this.payments = payments;
        this.gateway = gateway;
        this.processor = processor;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${sentinel.reconciler.interval:30s}")
    public void reconcile() {
        reconcileOnce(Instant.now());
    }

    public void reconcileOnce(Instant now) {
        for (UUID id : payments.unresolvedBefore(now.minus(props.payment().reconcileAfter()))) {
            try {
                var known = gateway.lookup("charge:" + id);
                if (known.isPresent()) {
                    processor.applyChargeResult(id, known.get(), null);
                } else {
                    // No charge at the provider. If the reservation is already terminal, no further charge
                    // attempt can happen (the consumer checks status first), so close the payment out.
                    processor.voidIfReservationTerminal(id);
                }
            } catch (RuntimeException e) {
                log.warn("Reconcile failed for {}: {}", id, e.toString());
            }
        }
        for (UUID id : payments.capturedButNotConfirmed()) {
            try {
                processor.refundIfCaptured(id);
            } catch (RuntimeException e) {
                log.warn("Refund during reconcile failed for {}: {}", id, e.toString());
            }
        }
    }
}
