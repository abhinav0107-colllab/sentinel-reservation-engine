package com.sentinel.reservation.reservation;

import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/reservations")
public class ReservationController {

    public static final String IDEMPOTENCY_HEADER = "Idempotency-Key";
    public static final String REPLAY_HEADER = "Idempotent-Replayed";

    private final ReservationService service;

    public ReservationController(ReservationService service) {
        this.service = service;
    }

    /**
     * 201 hold created (status PENDING_PAYMENT; payment completes asynchronously),
     * 201 + Idempotent-Replayed: true for a retry of a committed request,
     * 409 sold out, 422 key reused with a different body, 400 missing/invalid key or body.
     */
    @PostMapping
    public ResponseEntity<String> create(@RequestHeader(name = IDEMPOTENCY_HEADER, required = false) String key,
                                         @Valid @RequestBody CreateReservationRequest request) {
        if (key == null || key.isBlank() || key.length() > 100 || !key.matches("[A-Za-z0-9._:-]+")) {
            throw new IllegalArgumentException(IDEMPOTENCY_HEADER + " header is required (1-100 chars of [A-Za-z0-9._:-])");
        }
        var result = service.create(key, request);
        return ResponseEntity.status(result.httpStatus())
                .contentType(MediaType.APPLICATION_JSON)
                .header(REPLAY_HEADER, String.valueOf(result.replayed()))
                .body(result.body());
    }

    @GetMapping("/{id}")
    public ReservationService.ReservationView get(@PathVariable UUID id) {
        return service.get(id);
    }

    @PostMapping("/{id}/cancel")
    public ReservationService.ReservationView cancel(@PathVariable UUID id) {
        return service.cancel(id);
    }
}
