# Sentinel — Inventory & Reservation Engine

A hotel reservation engine that **cannot oversell**, survives client retries and process crashes, and
completes payment through an orchestrated saga with compensation. PostgreSQL is the single source of
truth; Kafka carries reservation events through a transactional outbox; a deterministic payment
simulator injects declines, outages and ambiguous timeouts on demand.

Java 21 · Spring Boot 3.5 · PostgreSQL 16 · Kafka (KRaft) · Docker Compose · Testcontainers ·
GitHub Actions · Prometheus/Grafana · Python tooling

```mermaid
flowchart LR
    Guest -->|POST /reservations + Idempotency-Key| S[Sentinel]
    S -->|1 tx: key + ordered row locks + reservation + outbox| PG[(PostgreSQL)]
    S -->|relay| K{{Kafka}} --> P[Payment step] -->|key charge:id| X[Provider sim]
    P -->|confirm / compensate| PG
    Sweeper -->|expire holds| PG
```

Implementation report: [`docs/IMPLEMENTATION_REPORT.md`](docs/IMPLEMENTATION_REPORT.md) · Design document: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) ·
API: [`docs/API.md`](docs/API.md) ·
Concurrency evidence: [`docs/CONCURRENCY_EVIDENCE.md`](docs/CONCURRENCY_EVIDENCE.md)

## Engineering highlights

- **No oversell, by construction.** Nights are locked with `SELECT … ORDER BY stay_date FOR UPDATE`
  (deterministic order ⇒ no deadlocks between overlapping stays), checked, then decremented. A
  `CHECK (reserved <= total)` constraint is the backstop.
- **Verified with a negative control.** A database-level concurrency check runs the production SQL from
  32 concurrent connections and confirms the invariants. The same checker catches a naive implementation
  that accepted 356 reservations against 10 rooms per night without ever tripping the CHECK constraint.
- **Idempotency keys done properly.** Stored in PostgreSQL in the same transaction as the booking;
  concurrent duplicates are serialized by the unique index; key reuse with a different body → 422.
- **Transactional outbox + idempotent consumers.** Events commit with state; consumers deduplicate with
  `processed_events`; every provider call carries an idempotency key.
- **Saga with compensation.** Decline → release rooms. Capture after expiry → refund. Provider timeout →
  retry with the same key (no double charge). Provider down → hold expiry + reconciliation converge.
- **Exactly-once release.** An explicit `holds_inventory` flag means cancel, expiry and payment failure
  can race and rooms are still returned exactly once.

## Status — what is verified and what is not

Status as of 2026-11-21. "Executed" means the check was actually run and passed; anything else is
labelled. Full numbers, environment and caveats: [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

| Area | Status | Evidence |
|---|---|---|
| Java application compiles | ✅ Executed | `mvn verify`, Java 21 / Spring Boot 3.5.7. No code changes were needed |
| Java tests: 1 unit class, 3 integration classes (HTTP-level concurrency, idempotency, 7 saga/failure-injection scenarios) | ✅ Executed | 2 unit + 14 integration tests passed (two consecutive runs). External mode against local PostgreSQL 16, embedded KRaft Kafka. **Not** run via Testcontainers (no Docker) |
| Reservation SQL under concurrency: no oversell, no drift, no deadlocks, idempotent retries, negative control detected | ✅ Executed (re-run) | [`reports/sql-concurrency-rerun-2026-11-21.json`](reports/sql-concurrency-rerun-2026-11-21.json); original: [`reports/sql-concurrency.json`](reports/sql-concurrency.json), [`docs/CONCURRENCY_EVIDENCE.md`](docs/CONCURRENCY_EVIDENCE.md) |
| Schema (Flyway V1) | ✅ Executed | Applied by Flyway on app and test start |
| Flash-sale load test (50 rooms, 32 clients, 30 s) | ✅ Executed | 1,473 req/s, p95 41.7 ms, 0 unexpected errors; exactly 50 confirmed; 0 oversold / drifted nights (checked in SQL independently of the app) |
| Spread load test (200 room types, 32 clients, 60 s) | ✅ Executed | 1,730 req/s, p95 37.0 ms, 0 unexpected errors; 0 oversold / drifted nights |
| Saga end state after load (confirm / decline / expiry / late-capture refund) | ✅ Executed | Every hold settled; 11 late captures refunded; provider ledger reconciles to the cent |
| Payment step throughput | ⚠️ Finding | ~17 payments/s (one listener thread, 50 ms simulated provider) vs ~1,000 holds/s accepted. 83 % of spread-run holds expired before payment. Not fixed |
| Docker Compose stack, Dockerfile | 🟡 Not run | No Docker on the verification machine (`docker compose config` was validated earlier) |
| GitHub Actions CI | 🟡 Written, not yet run | Requires pushing to GitHub |
| Grafana dashboard | 🟡 Provisioned JSON, not viewed against live metrics | |
| Provider outage / reconciler against the running service | ⬜ Not run | Covered only by `PaymentSagaIT` |

### Features

| Feature | State |
|---|---|
| Hotels, room types, per-night inventory and price (admin API) | Implemented |
| Lock-free availability read | Implemented |
| Reservation creation with ordered row locking + CHECK backstop | Implemented |
| Cancellation (idempotent), with refund of captured payments | Implemented |
| Idempotency keys (same-tx, hash check, concurrent-safe) | Implemented |
| Transactional outbox + relay (SKIP LOCKED) | Implemented |
| Kafka event publishing, consumer dedup, retry + DLT | Implemented |
| Simulated payment provider with failure injection | Implemented |
| Saga compensation (decline, late capture, cancel-after-confirm) | Implemented |
| Hold expiry sweeper, payment reconciler | Implemented |
| Inventory invariant audit endpoint | Implemented |
| Guest authentication, per-client key scoping | Not implemented |
| Distributed tracing | Not implemented |
| Hot-row mitigation (admission queue) | Not implemented |
| Scheduled idempotency-key cleanup | Partially (repository method exists, not scheduled) |

## Run it

```bash
cp .env.example .env              # set real values; .env is git-ignored
docker compose up -d --build      # postgres, kafka, sentinel on :8081
docker compose --profile observability up -d    # Prometheus :9091, Grafana :3001
```

Walkthrough (see `docs/API.md` for full examples):

```bash
KEY=...   # SENTINEL_ADMIN_API_KEY
RT=$(curl -s -X POST localhost:8081/api/v1/admin/hotels -H "X-Api-Key: $KEY" -H 'Content-Type: application/json' \
      -d '{"name":"Demo","roomTypes":[{"name":"Deluxe","maxGuests":2}]}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["roomTypes"][0]["roomTypeId"])')
curl -s -X PUT localhost:8081/api/v1/admin/room-types/$RT/inventory -H "X-Api-Key: $KEY" -H 'Content-Type: application/json' \
      -d '{"from":"2027-03-01","to":"2027-03-10","total":2,"price":"150.00","currency":"USD"}'
curl -s -X POST localhost:8081/api/v1/reservations -H "Idempotency-Key: demo-1" -H 'Content-Type: application/json' \
      -d "{\"roomTypeId\":\"$RT\",\"checkIn\":\"2027-03-01\",\"checkOut\":\"2027-03-03\",\"rooms\":1,\"guestName\":\"Ada\",\"guestEmail\":\"ada@example.com\",\"paymentToken\":\"tok_ok\"}"
```

## Test

```bash
mvn verify                                              # unit + integration (Testcontainers: needs Docker)
python3 tools/sql_concurrency_check.py --dsn postgresql://postgres@localhost:5432/sentinel_check
```

Without Docker: `SENTINEL_IT_EXTERNAL=true SENTINEL_IT_DB_URL=jdbc:postgresql://localhost:5432/sentinel_it SENTINEL_IT_DB_USER=postgres SENTINEL_IT_DB_PASSWORD=... mvn verify` (Kafka is embedded).

## Benchmarks

```bash
python3 tools/sentinel_loadtest.py --scenario flash-sale --rooms 50 --concurrency 32 --duration 30 --out reports/flash.json
python3 tools/sentinel_loadtest.py --scenario spread --room-types 200 --concurrency 32 --duration 60 --out reports/spread.json
```

Each report includes latency percentiles, status counts, unexpected-error rate and the post-run inventory
audit result. For an audit that does not go through the application, run
`psql -d sentinel -f tools/post_load_check.sql`. Results: **[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md)**.

## Layout

```
src/main/java/com/sentinel/reservation/
  inventory/    inventory SQL (ordered locks), admin + availability API, audit
  reservation/  state machine service, idempotency, API, hold expiry sweeper
  payment/      provider port, simulator, saga step (consumer), reconciler
  outbox/       outbox writer, relay
  config/       properties, Kafka, errors, API key
tools/          SQL concurrency check (+ negative control), load generator
```
