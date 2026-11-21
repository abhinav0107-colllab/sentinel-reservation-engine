package com.sentinel.reservation.reservation;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.LocalDate;
import java.util.UUID;

public record CreateReservationRequest(
        @NotNull UUID roomTypeId,
        @NotNull @Future LocalDate checkIn,
        @NotNull LocalDate checkOut,
        @NotNull @Min(1) @Max(9) Integer rooms,
        @NotBlank @Size(max = 200) String guestName,
        @NotBlank @Email @Size(max = 320) String guestEmail,
        /** Token issued by the payment provider's client SDK; card data never reaches this service. */
        @NotBlank @Pattern(regexp = "tok_[a-z_]{1,60}") String paymentToken) {
}
