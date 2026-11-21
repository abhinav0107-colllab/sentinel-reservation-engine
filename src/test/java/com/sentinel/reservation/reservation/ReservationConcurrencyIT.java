package com.sentinel.reservation.reservation;

import com.sentinel.reservation.support.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves, through the real HTTP API and a real PostgreSQL, that concurrent reservation attempts cannot
 * oversell inventory, and that inventory accounting stays exact while cancellations run concurrently.
 */
class ReservationConcurrencyIT extends IntegrationTestBase {

    private static final LocalDate IN = LocalDate.now().plusDays(30);

    @Test
    void lastRoomsRace_exactlyCapacitySucceeds() throws Exception {
        int capacity = 10, attempts = 120;
        UUID rt = roomTypeWithInventory(capacity, IN, IN.plusDays(3), "100.00");

        ExecutorService pool = Executors.newFixedThreadPool(40);
        CountDownLatch go = new CountDownLatch(1);
        Map<Integer, AtomicInteger> statuses = new ConcurrentHashMap<>();
        for (int i = 0; i < attempts; i++) {
            pool.submit(() -> {
                go.await();
                int code = reserve(UUID.randomUUID().toString(), rt, IN, IN.plusDays(3), 1, "tok_provider_down")
                        .getStatusCode().value();
                statuses.computeIfAbsent(code, c -> new AtomicInteger()).incrementAndGet();
                return null;
            });
        }
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

        assertThat(statuses.get(201).get()).isEqualTo(capacity);
        assertThat(statuses.get(409).get()).isEqualTo(attempts - capacity);
        assertThat(statuses.keySet()).containsOnly(201, 409);
        for (int d = 0; d < 3; d++) {
            assertThat(reserved(rt, IN.plusDays(d))).isEqualTo(capacity);
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM reservations WHERE holds_inventory", Integer.class)).isEqualTo(capacity);
        assertThat(inventoryConsistent()).isTrue();
    }

    @Test
    void overlappingMultiNightStaysWithConcurrentCancellations_neverOversellOrDrift() throws Exception {
        int capacity = 8, nights = 6, threads = 32, perThread = 25;
        UUID rt = roomTypeWithInventory(capacity, IN, IN.plusDays(nights), "80.00");
        List<UUID> held = Collections.synchronizedList(new ArrayList<>());
        Map<Integer, AtomicInteger> statuses = new ConcurrentHashMap<>();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            int seed = t;
            pool.submit(() -> {
                Random rnd = new Random(seed);
                go.await();
                for (int i = 0; i < perThread; i++) {
                    if (!held.isEmpty() && rnd.nextDouble() < 0.25) {
                        UUID victim;
                        synchronized (held) {
                            victim = held.get(rnd.nextInt(held.size()));
                        }
                        int code = rest.exchange("/api/v1/reservations/" + victim + "/cancel", HttpMethod.POST,
                                HttpEntity.EMPTY, String.class).getStatusCode().value();
                        statuses.computeIfAbsent(1000 + code, c -> new AtomicInteger()).incrementAndGet();
                        continue;
                    }
                    LocalDate in = IN.plusDays(rnd.nextInt(nights));
                    LocalDate out = in.plusDays(1 + rnd.nextInt(3));
                    if (out.isAfter(IN.plusDays(nights))) out = IN.plusDays(nights);
                    var resp = reserve(UUID.randomUUID().toString(), rt, in, out, 1 + rnd.nextInt(2), "tok_provider_down");
                    statuses.computeIfAbsent(resp.getStatusCode().value(), c -> new AtomicInteger()).incrementAndGet();
                    if (resp.getStatusCode().value() == 201) {
                        held.add(UUID.fromString(json(resp.getBody()).path("id").asText()));
                    }
                }
                return null;
            });
        }
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(120, TimeUnit.SECONDS)).isTrue();

        // Only expected outcomes: created, sold out, cancel OK. Never 5xx.
        assertThat(statuses.keySet()).allMatch(c -> c == 201 || c == 409 || c == 1200);
        assertThat(statuses.get(201).get()).isGreaterThan(0);
        assertThat(inventoryConsistent()).as("reserved == rooms held by active reservations, and <= total").isTrue();
        Integer maxReserved = jdbc.queryForObject("SELECT max(reserved - total) FROM room_inventory WHERE room_type_id = ?", Integer.class, rt);
        assertThat(maxReserved).isLessThanOrEqualTo(0);
    }
}
