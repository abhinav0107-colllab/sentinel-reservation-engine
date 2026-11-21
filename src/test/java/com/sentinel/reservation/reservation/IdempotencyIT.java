package com.sentinel.reservation.reservation;

import com.sentinel.reservation.support.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class IdempotencyIT extends IntegrationTestBase {

    private static final LocalDate IN = LocalDate.now().plusDays(40);

    @Test
    void concurrentRetriesWithSameKeyCreateExactlyOneReservation() throws Exception {
        UUID rt = roomTypeWithInventory(5, IN, IN.plusDays(2), "120.00");
        String key = "client-retry-" + UUID.randomUUID();
        int n = 20;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<ResponseEntity<String>>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Callable<ResponseEntity<String>> call = () -> {
                go.await();
                return reserve(key, rt, IN, IN.plusDays(2), 1, "tok_provider_down");
            };
            futures.add(pool.submit(call));
        }
        go.countDown();
        List<ResponseEntity<String>> responses = new ArrayList<>();
        for (var f : futures) responses.add(f.get());
        pool.shutdown();

        assertThat(responses).allMatch(r -> r.getStatusCode().value() == 201);
        assertThat(responses.stream().map(ResponseEntity::getBody).distinct()).hasSize(1);
        assertThat(responses.stream().filter(r -> "true".equals(r.getHeaders().getFirst("Idempotent-Replayed"))).count())
                .isEqualTo(n - 1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM reservations", Integer.class)).isEqualTo(1);
        assertThat(reserved(rt, IN)).isEqualTo(1);
    }

    @Test
    void sameKeyDifferentBodyIsRejected() throws Exception {
        UUID rt = roomTypeWithInventory(5, IN, IN.plusDays(3), "120.00");
        String key = "k-" + UUID.randomUUID();
        assertThat(reserve(key, rt, IN, IN.plusDays(2), 1, "tok_provider_down").getStatusCode().value()).isEqualTo(201);
        var reuse = reserve(key, rt, IN, IN.plusDays(3), 1, "tok_provider_down");
        assertThat(reuse.getStatusCode().value()).isEqualTo(422);
        assertThat(reuse.getBody()).contains("idempotency-key-reuse");
        assertThat(reserved(rt, IN.plusDays(2))).isZero();
    }

    @Test
    void missingKeyIsRejected() throws Exception {
        UUID rt = roomTypeWithInventory(5, IN, IN.plusDays(2), "120.00");
        assertThat(reserve(null, rt, IN, IN.plusDays(2), 1, "tok_ok").getStatusCode().value()).isEqualTo(400);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM reservations", Integer.class)).isZero();
    }

    @Test
    void failedAttemptDoesNotBurnTheKey() throws Exception {
        UUID rt = roomTypeWithInventory(0, IN, IN.plusDays(2), "120.00");
        String key = "k-" + UUID.randomUUID();
        assertThat(reserve(key, rt, IN, IN.plusDays(2), 1, "tok_provider_down").getStatusCode().value()).isEqualTo(409);

        rest.exchange("/api/v1/admin/room-types/" + rt + "/inventory", HttpMethod.PUT, new HttpEntity<>(Map.of(
                "from", IN.toString(), "to", IN.plusDays(2).toString(), "total", 3, "price", "120.00", "currency", "USD"),
                admin()), String.class);
        var retry = reserve(key, rt, IN, IN.plusDays(2), 1, "tok_provider_down");
        assertThat(retry.getStatusCode().value()).isEqualTo(201);
        assertThat(retry.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("false");
    }

    @Test
    void inventoryCannotBeReducedBelowReserved() throws Exception {
        UUID rt = roomTypeWithInventory(2, IN, IN.plusDays(1), "120.00");
        reserve("a-" + UUID.randomUUID(), rt, IN, IN.plusDays(1), 2, "tok_provider_down");
        var shrink = rest.exchange("/api/v1/admin/room-types/" + rt + "/inventory", HttpMethod.PUT, new HttpEntity<>(Map.of(
                "from", IN.toString(), "to", IN.plusDays(1).toString(), "total", 1, "price", "120.00", "currency", "USD"),
                admin()), String.class);
        assertThat(shrink.getStatusCode().value()).isEqualTo(409);
        assertThat(inventoryConsistent()).isTrue();
    }
}
