# Sentinel — Benchmarks and Verification Results

Every number below was produced by a command that was actually run on 2026-11-21 against the code at
commit `f997a2b` (application code unchanged from the original implementation; only the test base
changed). Raw JSON reports are in [`reports/`](../reports). Nothing here is estimated or extrapolated.
Single-machine results: they describe this machine, not production capacity.

## Environment

| | |
|---|---|
| Machine | 1 VM, 4 vCPU (Intel Xeon @ 2.10 GHz), 15 GiB RAM, Linux 6.18 |
| Topology | App, PostgreSQL, Kafka **and the load client** on one host |
| Java | OpenJDK 21.0.13, app run with `-Xmx1g` (`java -jar`, not Docker) |
| PostgreSQL | 16.15 (local package), `max_connections=300` |
| Kafka | 3.9.1, single-node KRaft (Apache tarball, SHA-512 verified) |
| App config | Defaults from `application.yml`: hold TTL 10 min, simulated payment latency 50 ms, Hikari pool 30 |
| Load client | `tools/sentinel_loadtest.py` (Python 3.13, stdlib, closed-loop) |

There was no Docker on this machine, so the Compose stack and the Testcontainers path were **not** used.

## 1. Build and tests (`mvn verify`)

Integration tests ran in external mode (`SENTINEL_IT_EXTERNAL=true`) against the local PostgreSQL, with
Kafka embedded (`@EmbeddedKafka`, KRaft).

| Suite | Tests | Result |
|---|---|---|
| Unit (`ReservationServiceTest`) | 2 | 2 passed |
| `PaymentSagaIT` (happy path, decline compensation, ambiguous timeout retry without double charge, provider outage → expiry + void, cancel after confirm → refund, late capture after expiry → refund, duplicate delivery) | 7 | 7 passed |
| `ReservationConcurrencyIT` (HTTP last-rooms race; overlapping stays with concurrent cancels) | 2 | 2 passed |
| `IdempotencyIT` (concurrent same-key retries, key reuse with different body, missing key, failed attempt does not burn key, shrink below reserved) | 5 | 5 passed |

The application code compiled and every test passed on the first build. One test-infrastructure change
was made: `@EmbeddedKafka` was running a ZooKeeper broker although the docs say KRaft, so `kraft = true`
was set. The final code passed `mvn verify` in two consecutive runs.

## 2. SQL-level concurrency check (re-run)

```bash
python3 tools/sql_concurrency_check.py --dsn postgresql://postgres:***@localhost:5432/sentinel_check \
    --out reports/sql-concurrency-rerun-2026-11-21.json
```

Report: [`reports/sql-concurrency-rerun-2026-11-21.json`](../reports/sql-concurrency-rerun-2026-11-21.json)
(the original run is kept in `reports/sql-concurrency.json`).

| Scenario | Outcomes | Oversold nights | Accounting mismatches |
|---|---|---|---|
| 32 threads × 50 ops, 10 rooms × 5 nights, 20 % cancels | 148 held, 109 released, 1,146 sold out, 197 release no-ops | 0 | 0 |
| Same, no cancels | 29 held, 1,571 sold out | 0 | 0 |
| 25 concurrent requests, one idempotency key | 1 created, 24 replayed, 1 distinct id | — | — |
| Negative control (naive read-then-write) | **350 holds accepted** for 10 rooms/night | 0 (counter stayed at 10) | **5 of 5 nights** — detected |

## 3. Load tests against the running service

After each run the generator waits for the inventory audit endpoint to report consistent. In addition,
the invariants were checked **independently in SQL** with [`tools/post_load_check.sql`](../tools/post_load_check.sql)
(not via the application): no night with `reserved > total`; `reserved` equals the rooms held by
reservations with `holds_inventory`; no confirmed reservation without a captured payment; no unpublished
outbox rows.

Each request is a new booking with a fresh `Idempotency-Key`; 10 % use a declining payment token.
Closed-loop, 32 workers, latency measured by the client on the same host. HTTP 409 = sold out.

### 3.1 Flash sale — 50 rooms, everyone wants the same 2 nights

```bash
python3 tools/sentinel_loadtest.py --scenario flash-sale --rooms 50 --concurrency 32 --duration 30 --out reports/flash.json
```

| Metric | Value |
|---|---|
| Requests (30 s) | 44,222 → **1,473 req/s** |
| Latency p50 / p95 / p99 / max | 18.95 / 41.7 / 68.45 / 694.78 ms |
| Status | 56 × 201, 44,166 × 409 (sold out), **0 unexpected** |
| Final state | **50 CONFIRMED** (= capacity), 6 PAYMENT_FAILED (declined → rooms released and resold) |
| Oversold nights / drifted nights | 0 / 0 |
| Audit endpoint | consistent |

Almost all requests are fast sold-out rejections, so the throughput figure mostly measures how quickly
the service refuses once inventory is gone. The point of this run is correctness under contention.

### 3.2 Spread — 200 room types × 50 rooms × 14 nights, 1–3-night stays

```bash
python3 tools/sentinel_loadtest.py --scenario spread --room-types 200 --concurrency 32 --duration 60 --out reports/spread.json
```

| Metric | Value |
|---|---|
| Requests (60 s) | 103,887 → **1,730 req/s** |
| Latency p50 / p95 / p99 / max | 16.39 / 37.03 / 50.43 / 396.96 ms |
| Status | 59,692 × 201 (holds created), 44,195 × 409, **0 unexpected** |
| Oversold nights / drifted nights (right after the run) | 0 / 0 |

### 3.3 Payment saga backlog after the spread run (finding)

Right after the spread run, 58,976 of its 59,692 holds were still `PENDING_PAYMENT`.

| Observation | Value |
|---|---|
| Payment step throughput (measured over 60 s, no load) | **17.3 reservations/s** |
| Cause | One listener thread consumes all 3 partitions (no `concurrency` set) and calls the simulated provider (50 ms latency) synchronously → ceiling ≈ 20/s |
| Consequence | Holds outlived the 10-minute TTL before their payment was attempted |

The backlog was left to drain (10:07 → 10:19 UTC) without intervention. This exercised the expiry and
late-capture paths at volume. End state, checked independently in SQL and against the simulated
provider's ledger. Counts cover the whole database, i.e. the 59,692 spread holds **plus** the 56 from
the flash-sale run (50 confirmed, 6 failed):

| Final state | Count |
|---|---|
| CONFIRMED (payment CAPTURED) | 9,225 |
| EXPIRED, no payment row (hold expired before the payment step reached it; never charged) | 49,507 |
| EXPIRED, captured just after expiry → **REFUNDED** | 11 |
| PAYMENT_FAILED (DECLINED, rooms released) | 1,005 |
| Still PENDING_PAYMENT / unpublished outbox rows | 0 / 0 |
| Oversold nights / drifted nights | 0 / 0 |
| Provider ledger | 9,236 captures − 11 refunds = 9,225 confirmed; $1,834,700 − $2,400 = $1,832,300 = sum of CAPTURED payments |
| WARN/ERROR lines in app log | 0 |

**Interpretation.** Correctness held: rooms were never oversold, and every hold ended either confirmed
with a captured payment, expired with no charge or a refund, or failed with rooms released. But the
payment step is the throughput bottleneck by about two orders of magnitude (≈17/s vs ≈1,000 holds/s
accepted). In this run 49,518 of the 59,692 spread holds (83 %) expired. With a real provider, bookings would need either
parallel payment processing (listener concurrency, async provider calls) or admission control. Not fixed.

## Not measured

- Docker Compose stack, GitHub Actions CI, Grafana dashboard against live metrics.
- Provider outage / failure-injection scenarios against the running service (they are covered only by `PaymentSagaIT`).
- Payment reconciler at volume (no UNKNOWN payments occurred in these runs).
- Multi-instance deployment.
