package com.sentinel.reservation.reservation;

/** Domain exceptions, each mapped to one HTTP status in ApiErrorHandler. */
public final class ReservationExceptions {
    private ReservationExceptions() {}

    /** 409: not enough rooms on at least one night (or the room type has no inventory for those dates). */
    public static class SoldOut extends RuntimeException {
        public SoldOut(String message) { super(message); }
    }

    /** 422: the Idempotency-Key was already used with a different request body. */
    public static class KeyReuse extends RuntimeException {
        public KeyReuse() { super("Idempotency-Key was already used with a different request"); }
    }

    /** 404 */
    public static class NotFound extends RuntimeException {
        public NotFound(String message) { super(message); }
    }

    /** 409: the requested transition is not allowed from the current state. */
    public static class InvalidState extends RuntimeException {
        public InvalidState(String message) { super(message); }
    }
}
