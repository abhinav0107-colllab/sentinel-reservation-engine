package com.sentinel.reservation.payment;

import com.sentinel.reservation.config.SentinelProperties;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

/**
 * In-process stand-in for an external payment provider, with deterministic failure injection driven by
 * the payment token (so tests and demos can trigger each failure mode on purpose):
 *
 * <ul>
 *   <li>{@code tok_decline}                 – declined (card_declined)</li>
 *   <li>{@code tok_timeout_after_capture}   – the FIRST call captures the money and then times out (the
 *       classic ambiguous failure); later calls with the same key return the original capture</li>
 *   <li>{@code tok_provider_down}           – every call times out and nothing is captured</li>
 *   <li>anything else                       – captured</li>
 * </ul>
 *
 * Its ledger lives in its own table and is written in its own transactions (REQUIRES_NEW), modelling a
 * separate system whose state is not rolled back with ours.
 */
@Component
public class SimulatedPaymentGateway implements PaymentGateway {

    private final JdbcClient jdbc;
    private final TransactionTemplate providerTx;
    private final SentinelProperties props;

    public SimulatedPaymentGateway(JdbcClient jdbc, PlatformTransactionManager tm, SentinelProperties props) {
        this.jdbc = jdbc;
        this.providerTx = new TransactionTemplate(tm);
        this.providerTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.props = props;
    }

    @Override
    public ChargeResult charge(String key, BigDecimal amount, String currency, String token) {
        simulateLatency();
        Optional<ChargeResult> previous = lookup(key);
        if (previous.isPresent()) {
            return previous.get(); // provider-side idempotency
        }
        if ("tok_provider_down".equals(token)) {
            throw new PaymentTimeoutException("provider timed out (simulated outage)");
        }
        boolean decline = "tok_decline".equals(token);
        String ref = (decline ? "dcl_" : "ch_") + UUID.randomUUID();
        record(key, "charge", decline ? "DECLINED" : "CAPTURED", ref, amount);
        if ("tok_timeout_after_capture".equals(token)) {
            throw new PaymentTimeoutException("provider timed out after capturing (simulated)");
        }
        // Re-read: if a concurrent duplicate call won the insert race, both callers must see ITS result.
        return lookup(key).orElseThrow();
    }

    @Override
    public String refund(String key, String providerRef, BigDecimal amount) {
        simulateLatency();
        return providerTx.execute(s -> {
            Optional<String> existing = jdbc.sql("SELECT provider_ref FROM simulated_provider_ledger WHERE idem_key = :k")
                    .param("k", key).query(String.class).optional();
            if (existing.isPresent()) {
                return existing.get();
            }
            String ref = "re_" + UUID.randomUUID();
            jdbc.sql("""
                    INSERT INTO simulated_provider_ledger (idem_key, operation, result, provider_ref, amount)
                    VALUES (:k, 'refund', 'REFUNDED', :ref, :amt) ON CONFLICT DO NOTHING
                    """).param("k", key).param("ref", ref).param("amt", amount).update();
            return ref;
        });
    }

    @Override
    public Optional<ChargeResult> lookup(String key) {
        return providerTx.execute(s -> jdbc.sql(
                        "SELECT result, provider_ref FROM simulated_provider_ledger WHERE idem_key = :k AND operation = 'charge'")
                .param("k", key)
                .query((rs, i) -> "CAPTURED".equals(rs.getString("result"))
                        ? new ChargeResult(Outcome.CAPTURED, rs.getString("provider_ref"), null)
                        : new ChargeResult(Outcome.DECLINED, rs.getString("provider_ref"), "card_declined"))
                .optional());
    }

    private void record(String key, String op, String result, String ref, BigDecimal amount) {
        providerTx.executeWithoutResult(s -> jdbc.sql("""
                INSERT INTO simulated_provider_ledger (idem_key, operation, result, provider_ref, amount)
                VALUES (:k, :op, :res, :ref, :amt) ON CONFLICT DO NOTHING
                """).param("k", key).param("op", op).param("res", result).param("ref", ref).param("amt", amount).update());
    }

    private void simulateLatency() {
        long ms = props.payment().latency().toMillis();
        if (ms > 0) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PaymentTimeoutException("interrupted");
            }
        }
    }
}
