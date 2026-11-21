-- Sentinel schema v1
-- PostgreSQL is the single source of truth for inventory. Correctness of "never oversell" rests on
-- (1) row locks taken in a deterministic order and (2) the CHECK constraint below as a last line of
-- defence: even a buggy code path cannot commit reserved > total.

CREATE TABLE hotels (
    id          UUID PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    timezone    VARCHAR(64)  NOT NULL DEFAULT 'UTC',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE room_types (
    id          UUID PRIMARY KEY,
    hotel_id    UUID         NOT NULL REFERENCES hotels (id),
    name        VARCHAR(100) NOT NULL,
    max_guests  SMALLINT     NOT NULL CHECK (max_guests BETWEEN 1 AND 20),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (hotel_id, name)
);

-- One row per room type per night: the atomic unit of inventory. A 3-night stay touches 3 rows.
CREATE TABLE room_inventory (
    room_type_id  UUID          NOT NULL REFERENCES room_types (id),
    stay_date     DATE          NOT NULL,
    total         INTEGER       NOT NULL CHECK (total >= 0),
    reserved      INTEGER       NOT NULL DEFAULT 0,
    price         NUMERIC(10,2) NOT NULL CHECK (price > 0),
    currency      CHAR(3)       NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (room_type_id, stay_date),
    CONSTRAINT inventory_never_oversold CHECK (reserved >= 0 AND reserved <= total)
);

CREATE TABLE reservations (
    id               UUID PRIMARY KEY,
    hotel_id         UUID          NOT NULL REFERENCES hotels (id),
    room_type_id     UUID          NOT NULL REFERENCES room_types (id),
    check_in         DATE          NOT NULL,
    check_out        DATE          NOT NULL,
    rooms            SMALLINT      NOT NULL CHECK (rooms BETWEEN 1 AND 9),
    guest_name       VARCHAR(200)  NOT NULL,
    guest_email      VARCHAR(320)  NOT NULL,
    amount           NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    currency         CHAR(3)       NOT NULL,
    payment_token    VARCHAR(64)   NOT NULL,     -- opaque token from the (simulated) payment provider
    status           VARCHAR(20)   NOT NULL CHECK (status IN
                        ('PENDING_PAYMENT', 'CONFIRMED', 'CANCELLED', 'EXPIRED', 'PAYMENT_FAILED')),
    -- Whether this reservation currently holds inventory. Kept explicitly so release is idempotent:
    -- inventory is returned exactly once, by whichever transition flips this to FALSE.
    holds_inventory  BOOLEAN       NOT NULL DEFAULT TRUE,
    hold_expires_at  TIMESTAMPTZ   NOT NULL,
    failure_reason   VARCHAR(200),
    version          BIGINT        NOT NULL DEFAULT 1,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CHECK (check_out > check_in),
    CHECK (check_out - check_in <= 30),
    CHECK (NOT holds_inventory OR status IN ('PENDING_PAYMENT', 'CONFIRMED'))
);

-- Expiry sweeper scans only pending holds.
CREATE INDEX idx_reservations_pending_expiry ON reservations (hold_expires_at) WHERE status = 'PENDING_PAYMENT';

-- Idempotency keys are stored in the SAME database and written in the SAME transaction as the
-- reservation. If they lived in Redis, a crash between the DB commit and the Redis write would let a
-- retried request create a second reservation.
CREATE TABLE idempotency_keys (
    idem_key         VARCHAR(100)  NOT NULL,
    operation        VARCHAR(50)   NOT NULL,
    request_hash     CHAR(64)      NOT NULL,     -- SHA-256 of the canonical request; detects key reuse with a different body
    response_status  SMALLINT      NOT NULL,
    response_body    JSONB         NOT NULL,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (operation, idem_key)
);
CREATE INDEX idx_idempotency_created ON idempotency_keys (created_at);

-- Payments as seen by Sentinel.
CREATE TABLE payments (
    id               UUID PRIMARY KEY,
    reservation_id   UUID          NOT NULL UNIQUE REFERENCES reservations (id),
    status           VARCHAR(20)   NOT NULL CHECK (status IN ('PENDING', 'CAPTURED', 'DECLINED', 'REFUNDED', 'UNKNOWN')),
    amount           NUMERIC(12,2) NOT NULL,
    currency         CHAR(3)       NOT NULL,
    provider_ref     VARCHAR(100),
    attempts         INTEGER       NOT NULL DEFAULT 0,
    last_error       VARCHAR(500),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- The simulated external payment provider's own ledger. Kept in a separate table to model a separate
-- system that deduplicates on the merchant's idempotency key, as real providers do.
CREATE TABLE simulated_provider_ledger (
    idem_key         VARCHAR(100)  PRIMARY KEY,
    operation        VARCHAR(20)   NOT NULL,
    result           VARCHAR(20)   NOT NULL,
    provider_ref     VARCHAR(100)  NOT NULL,
    amount           NUMERIC(12,2) NOT NULL,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Transactional outbox (same pattern as Atlas).
CREATE TABLE outbox_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID          NOT NULL UNIQUE,
    aggregate_type  VARCHAR(50)   NOT NULL,
    aggregate_id    UUID          NOT NULL,
    event_type      VARCHAR(50)   NOT NULL,
    topic           VARCHAR(100)  NOT NULL,
    payload         JSONB         NOT NULL,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    attempts        INTEGER       NOT NULL DEFAULT 0,
    last_error      TEXT
);
CREATE INDEX idx_outbox_unpublished ON outbox_events (id) WHERE published_at IS NULL;

-- Consumer-side deduplication: a consumer records the event id in the same transaction as its side
-- effects. A redelivered event hits the primary key and is skipped.
CREATE TABLE processed_events (
    consumer      VARCHAR(50)  NOT NULL,
    event_id      UUID         NOT NULL,
    processed_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
