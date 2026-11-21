package com.sentinel.reservation.payment;

import java.math.BigDecimal;
import java.util.Optional;

/**
 * Port to an external payment provider. Every call carries a merchant idempotency key; the provider
 * guarantees that repeating a call with the same key returns the original result without charging
 * again (this is how Stripe, Adyen etc. behave). That property is what makes retrying an ambiguous
 * failure (timeout) safe.
 */
public interface PaymentGateway {

    enum Outcome { CAPTURED, DECLINED }

    record ChargeResult(Outcome outcome, String providerRef, String declineReason) {}

    /** @throws PaymentTimeoutException when the outcome is unknown (the charge may or may not have happened) */
    ChargeResult charge(String idempotencyKey, BigDecimal amount, String currency, String token);

    String refund(String idempotencyKey, String providerRef, BigDecimal amount);

    /** Ask the provider what happened to a previous call (used by reconciliation after a timeout). */
    Optional<ChargeResult> lookup(String idempotencyKey);

    class PaymentTimeoutException extends RuntimeException {
        public PaymentTimeoutException(String message) {
            super(message);
        }
    }
}
