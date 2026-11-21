package com.sentinel.reservation.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * PostgreSQL via Testcontainers (default, CI) or an external instance ({@code SENTINEL_IT_EXTERNAL=true},
 * {@code SENTINEL_IT_DB_URL}, {@code SENTINEL_IT_DB_USER}). Kafka runs in-JVM (KRaft) via @EmbeddedKafka.
 * Background schedulers are slowed down so tests drive expiry and reconciliation explicitly.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "sentinel.admin.api-key=" + IntegrationTestBase.API_KEY,
        "sentinel.payment.latency=0ms",
        "sentinel.outbox.poll-interval=50ms",
        "sentinel.sweeper.interval=1h",
        "sentinel.reconciler.interval=1h",
        "spring.datasource.hikari.maximum-pool-size=40",
})
@EmbeddedKafka(partitions = 3, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public abstract class IntegrationTestBase {

    public static final String API_KEY = "test-admin-key";
    static final boolean EXTERNAL = "true".equalsIgnoreCase(System.getenv("SENTINEL_IT_EXTERNAL"));
    static PostgreSQLContainer<?> postgres;

    static {
        if (!EXTERNAL) {
            if (!DockerClientFactory.instance().isDockerAvailable()) {
                throw new IllegalStateException("Docker unavailable: set SENTINEL_IT_EXTERNAL=true and SENTINEL_IT_DB_URL");
            }
            postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withCommand("postgres", "-c", "max_connections=200");
            postgres.start();
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        if (EXTERNAL) {
            r.add("spring.datasource.url", () -> env("SENTINEL_IT_DB_URL", "jdbc:postgresql://localhost:5432/sentinel_it"));
            r.add("spring.datasource.username", () -> env("SENTINEL_IT_DB_USER", "postgres"));
            r.add("spring.datasource.password", () -> env("SENTINEL_IT_DB_PASSWORD", ""));
        } else {
            r.add("spring.datasource.url", postgres::getJdbcUrl);
            r.add("spring.datasource.username", postgres::getUsername);
            r.add("spring.datasource.password", postgres::getPassword);
        }
    }

    private static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? d : v;
    }

    @Autowired protected TestRestTemplate rest;
    @Autowired protected JdbcTemplate jdbc;
    @Autowired protected ObjectMapper mapper;

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE processed_events, outbox_events, simulated_provider_ledger, payments, idempotency_keys, "
                + "reservations, room_inventory, room_types, hotels CASCADE");
    }

    protected HttpHeaders admin() {
        HttpHeaders h = new HttpHeaders();
        h.set("X-Api-Key", API_KEY);
        return h;
    }

    /** Creates a hotel with one room type and {@code rooms} rooms per night for [from, to). */
    protected UUID roomTypeWithInventory(int rooms, LocalDate from, LocalDate to, String price) throws Exception {
        var created = rest.exchange("/api/v1/admin/hotels", HttpMethod.POST, new HttpEntity<>(Map.of(
                "name", "Test Hotel " + UUID.randomUUID(),
                "roomTypes", List.of(Map.of("name", "Deluxe", "maxGuests", 2))), admin()), String.class);
        UUID rt = UUID.fromString(mapper.readTree(created.getBody()).path("roomTypes").get(0).path("roomTypeId").asText());
        var set = rest.exchange("/api/v1/admin/room-types/" + rt + "/inventory", HttpMethod.PUT, new HttpEntity<>(Map.of(
                "from", from.toString(), "to", to.toString(), "total", rooms, "price", price, "currency", "USD"), admin()), String.class);
        if (!set.getStatusCode().is2xxSuccessful()) throw new IllegalStateException(set.toString());
        return rt;
    }

    protected ResponseEntity<String> reserve(String key, UUID roomType, LocalDate in, LocalDate out, int rooms, String token) {
        HttpHeaders h = new HttpHeaders();
        if (key != null) h.set("Idempotency-Key", key);
        return rest.exchange("/api/v1/reservations", HttpMethod.POST, new HttpEntity<>(Map.of(
                "roomTypeId", roomType.toString(), "checkIn", in.toString(), "checkOut", out.toString(), "rooms", rooms,
                "guestName", "Ada Lovelace", "guestEmail", "ada@example.com", "paymentToken", token), h), String.class);
    }

    protected JsonNode json(String body) throws Exception {
        return mapper.readTree(body);
    }

    protected String status(UUID reservationId) {
        return jdbc.queryForObject("SELECT status FROM reservations WHERE id = ?", String.class, reservationId);
    }

    protected boolean inventoryConsistent() throws Exception {
        var audit = rest.exchange("/api/v1/admin/inventory/audit", HttpMethod.GET, new HttpEntity<>(admin()), String.class);
        return json(audit.getBody()).path("consistent").asBoolean();
    }

    protected int reserved(UUID rt, LocalDate night) {
        return jdbc.queryForObject("SELECT reserved FROM room_inventory WHERE room_type_id = ? AND stay_date = ?",
                Integer.class, rt, java.sql.Date.valueOf(night));
    }
}
