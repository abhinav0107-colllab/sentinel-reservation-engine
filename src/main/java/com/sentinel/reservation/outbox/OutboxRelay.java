package com.sentinel.reservation.outbox;

import com.sentinel.reservation.config.SentinelProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Polls the outbox and publishes to Kafka. Guarantees at-least-once delivery:
 * <ul>
 *   <li>A row is marked published only after the broker acknowledged it (acks=all, idempotent producer).</li>
 *   <li>If the process dies after the broker ack but before the DB commit, the row is re-sent on the next
 *       poll, producing a duplicate. Consumers deduplicate on eventId (processed_events) for that reason.</li>
 * </ul>
 * Polling was chosen over Debezium/CDC: one fewer moving part to operate, and the latency cost
 * (one poll interval) is acceptable for the payment saga. See docs/ARCHITECTURE.md.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final SentinelProperties props;
    private final Counter published;
    private final Counter failed;

    public OutboxRelay(OutboxRepository repository, KafkaTemplate<String, String> kafka, TransactionTemplate tx,
                       SentinelProperties props, MeterRegistry registry) {
        this.repository = repository;
        this.kafka = kafka;
        this.tx = tx;
        this.props = props;
        this.published = registry.counter("sentinel.outbox.published");
        this.failed = registry.counter("sentinel.outbox.publish.failures");
        Gauge.builder("sentinel.outbox.pending", repository, r -> safe(r::countUnpublished)).register(registry);
        Gauge.builder("sentinel.outbox.oldest.age.seconds", repository,
                r -> safe(() -> r.oldestUnpublishedAge().toMillis() / 1000.0)).register(registry);
    }

    @Scheduled(fixedDelayString = "${sentinel.outbox.poll-interval:100ms}")
    public void poll() {
        if (!props.outbox().relayEnabled()) {
            return;
        }
        int sent;
        do {
            sent = publishBatch();
        } while (sent == props.outbox().batchSize()); // drain quickly under backlog, then go back to sleep
    }

    /** @return number of rows successfully published in this batch */
    public int publishBatch() {
        Integer result = tx.execute(status -> {
            List<OutboxRepository.OutboxRow> rows = repository.lockNextBatch(props.outbox().batchSize());
            if (rows.isEmpty()) {
                return 0;
            }
            // Send the whole batch asynchronously, then wait: one broker round-trip per batch, not per row.
            List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>(rows.size());
            for (OutboxRepository.OutboxRow row : rows) {
                futures.add(kafka.send(row.topic(), row.aggregateId().toString(), row.payload()));
            }
            List<Long> ok = new ArrayList<>(rows.size());
            long timeoutMs = props.outbox().sendTimeout().toMillis();
            for (int i = 0; i < rows.size(); i++) {
                OutboxRepository.OutboxRow row = rows.get(i);
                try {
                    futures.get(i).get(timeoutMs, TimeUnit.MILLISECONDS);
                    ok.add(row.id());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    repository.markFailed(row.id(), "interrupted");
                } catch (Exception e) {
                    failed.increment();
                    log.warn("Outbox publish failed id={} eventId={} attempts={}: {}",
                            row.id(), row.eventId(), row.attempts() + 1, e.toString());
                    repository.markFailed(row.id(), e.toString());
                }
            }
            repository.markPublished(ok);
            published.increment(ok.size());
            return ok.size();
        });
        return result == null ? 0 : result;
    }

    @Scheduled(cron = "${sentinel.outbox.cleanup-cron:0 17 * * * *}")
    public void cleanup() {
        int deleted = repository.deletePublishedBefore(Instant.now().minus(props.outbox().retention()));
        if (deleted > 0) {
            log.info("Outbox cleanup removed {} published events", deleted);
        }
    }

    private static double safe(java.util.function.Supplier<Number> s) {
        try {
            return s.get().doubleValue();
        } catch (RuntimeException e) {
            return Double.NaN;
        }
    }
}
