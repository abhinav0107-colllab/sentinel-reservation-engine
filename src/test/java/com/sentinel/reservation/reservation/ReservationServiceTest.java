package com.sentinel.reservation.reservation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ReservationServiceTest {

    private final ReservationService service = new ReservationService(null, null, null, null,
            new ObjectMapper().findAndRegisterModules(), null, null);

    private static CreateReservationRequest req(int rooms, String token) {
        return new CreateReservationRequest(UUID.fromString("00000000-0000-0000-0000-000000000001"),
                LocalDate.of(2030, 1, 10), LocalDate.of(2030, 1, 12), rooms, "Ada", "ada@example.com", token);
    }

    @Test
    void requestHashIsStableAndSensitiveToEveryField() {
        assertThat(service.requestHash(req(1, "tok_ok"))).isEqualTo(service.requestHash(req(1, "tok_ok")));
        assertThat(service.requestHash(req(1, "tok_ok"))).isNotEqualTo(service.requestHash(req(2, "tok_ok")));
        assertThat(service.requestHash(req(1, "tok_ok"))).isNotEqualTo(service.requestHash(req(1, "tok_decline")));
        assertThat(service.requestHash(req(1, "tok_ok"))).hasSize(64);
    }

    @Test
    void terminalStates() {
        assertThat(Reservation.Status.PENDING_PAYMENT.isTerminal()).isFalse();
        assertThat(Reservation.Status.CONFIRMED.isTerminal()).isFalse();
        assertThat(Reservation.Status.CANCELLED.isTerminal()).isTrue();
        assertThat(Reservation.Status.EXPIRED.isTerminal()).isTrue();
        assertThat(Reservation.Status.PAYMENT_FAILED.isTerminal()).isTrue();
    }
}
