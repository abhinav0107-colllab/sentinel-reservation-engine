package com.sentinel.reservation.config;

import com.sentinel.reservation.reservation.ReservationExceptions;
import jakarta.validation.ConstraintViolationException;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.net.URI;
import java.util.stream.Collectors;

/** RFC 7807 problem responses with a stable machine-readable {@code type} per domain error. */
@RestControllerAdvice
public class ApiErrorHandler {

    private static ProblemDetail problem(HttpStatus status, String type, String detail) {
        ProblemDetail p = ProblemDetail.forStatusAndDetail(status, detail);
        p.setType(URI.create("urn:sentinel:error:" + type));
        return p;
    }

    @ExceptionHandler(ReservationExceptions.SoldOut.class)
    ProblemDetail soldOut(ReservationExceptions.SoldOut e) {
        return problem(HttpStatus.CONFLICT, "sold-out", e.getMessage());
    }

    @ExceptionHandler(ReservationExceptions.KeyReuse.class)
    ProblemDetail keyReuse(ReservationExceptions.KeyReuse e) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "idempotency-key-reuse", e.getMessage());
    }

    @ExceptionHandler(ReservationExceptions.NotFound.class)
    ProblemDetail notFound(ReservationExceptions.NotFound e) {
        return problem(HttpStatus.NOT_FOUND, "not-found", e.getMessage());
    }

    @ExceptionHandler(ReservationExceptions.InvalidState.class)
    ProblemDetail invalidState(ReservationExceptions.InvalidState e) {
        return problem(HttpStatus.CONFLICT, "invalid-state", e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail badRequest(IllegalArgumentException e) {
        return problem(HttpStatus.BAD_REQUEST, "bad-request", e.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail invalidBody(MethodArgumentNotValidException e) {
        String detail = e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage()).sorted().collect(Collectors.joining("; "));
        return problem(HttpStatus.BAD_REQUEST, "validation", detail);
    }

    @ExceptionHandler({ConstraintViolationException.class, HandlerMethodValidationException.class,
            MethodArgumentTypeMismatchException.class, HttpMessageNotReadableException.class})
    ProblemDetail invalidInput(Exception e) {
        return problem(HttpStatus.BAD_REQUEST, "validation", "Malformed or invalid request");
    }

    /** Lock timeout / deadlock (should not happen given lock ordering, but never surface as 500). */
    @ExceptionHandler(CannotAcquireLockException.class)
    ProblemDetail busy(CannotAcquireLockException e) {
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "busy", "Inventory is busy, please retry");
    }
}
