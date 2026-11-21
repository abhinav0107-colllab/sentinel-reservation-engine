package com.sentinel.reservation.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "sentinel")
public record SentinelProperties(
        @NotBlank String reservationTopic,
        @Min(1) int topicPartitions,
        Duration holdTtl,
        Outbox outbox,
        Payment payment,
        Admin admin) {

    public record Outbox(boolean relayEnabled, @Min(1) int batchSize, Duration sendTimeout, Duration retention) {}

    /**
     * Simulated payment provider behaviour. {@code latency} is added to every provider call so that
     * load tests exercise the asynchronous saga under realistic provider latency.
     */
    public record Payment(Duration latency, Duration reconcileAfter) {}

    public record Admin(String apiKey) {}
}
