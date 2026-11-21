# Sentinel — Implementation Report

> **Update 2026-11-21 (verification run):** the code has now been compiled and all tests executed
> (`mvn verify`: 2 unit + 14 integration tests pass, external PostgreSQL 16, embedded KRaft Kafka). No
> application code changes were needed. The service was load-tested (flash-sale and spread) with invariants
> checked independently in SQL. A payment-throughput bottleneck was found. See [`BENCHMARKS.md`](BENCHMARKS.md).
> The sections below are the original pre-compile report, kept for history.

Snapshot date: 2026-11-21. This report states only what was executed and observed. Anything not executed
is labelled as such.

## Build environment constraint

The development sandbox could not reach Maven Central (HTTP 403 from the egress proxy) and had no Docker
daemon, so **no Java code in this repository has been compiled or executed yet.** PostgreSQL 16, Java 21,
Maven, Python and PyPI (for `psycopg`) were available.

## Executed checks

| Check | Method | Result |
|---|---|---|
| Flyway V1 schema applies | `psql` on PostgreSQL 16.15 | Applied, 9 tables |
| Reservation SQL under concurrency | `tools/sql_concurrency_check.py`, 32 connections, production statements | No oversold night, no accounting drift, 0 deadlocks, with and without concurrent cancellations. Raw: `reports/sql-concurrency.json` |
| Idempotency under concurrent retries | same script, 25 concurrent requests with one key | 1 reservation, 24 replays, 1 distinct id |
| Negative control | same checker on a naive read-then-write variant | Violation detected (356 holds accepted for 10 rooms/night; counter never exceeded 10, so the CHECK constraint alone did not catch it) |
| Inventory upsert (`generate_series`), drift audit query, CHECK on shrink-below-reserved | `psql` | Upsert wrote 3 nights; audit flagged the injected drift; shrink rejected by `inventory_never_oversold` |
| Guarded payment status transitions | `psql` | `UNKNOWN→VOIDED` applied; `VOIDED→UNKNOWN` and `VOIDED→REFUNDED` rejected (0 rows) |
| Compose file | `docker compose config -q` | Valid (stack not started) |
| Python tools | `py_compile` | Both compile; load generator not yet run against a live service |
| Java syntax + intra-project references | `javac -proc:none` without dependencies | No parse errors; unresolved symbols are third-party types only |

## Implemented but not yet executed

- Java service: admin inventory API, availability, reservation creation with ordered row locking,
  idempotency (same tx, hash check), cancellation, outbox + relay, payment saga consumer with
  `processed_events` dedup, simulated provider with failure injection, hold-expiry sweeper, payment
  reconciler, inventory audit endpoint, RFC 7807 errors, metrics.
- Tests: `ReservationServiceTest` (unit); `ReservationConcurrencyIT` (HTTP-level last-room race and
  overlapping stays with cancels), `IdempotencyIT` (5 cases), `PaymentSagaIT` (7 saga / failure-injection
  scenarios).
- GitHub Actions (`mvn verify` + SQL concurrency job), Dockerfile, Grafana dashboard, load generator.

## Issues found by review and fixed

1. A Kafka retry in flight after the reconciler resolved a payment could regress its status → status
   updates now follow an explicit transition table in SQL (verified in psql).
2. Concurrent duplicate charges could return different provider refs → simulator re-reads the winning row.
3. Idempotent replays were not byte-identical because `jsonb` normalizes formatting → `response_body` is
   `TEXT`; SQL check re-run afterwards (passed).
4. Unknown payments with no provider record could stay unresolved forever → `VOIDED` once the reservation
   is terminal.

## Not implemented

Guest authentication and per-client idempotency-key scoping; hot-row mitigation; distributed tracing;
scheduled idempotency-key cleanup (method exists, not scheduled).

## Next steps to reach "verified"

1. Push to GitHub → CI runs `mvn verify` and the SQL concurrency job → fix failures.
2. Run `tools/sentinel_loadtest.py` (flash-sale and spread) against the compose stack; commit reports and
   quote them with environment details.
3. Promote README status items to ✅ only after they pass.
