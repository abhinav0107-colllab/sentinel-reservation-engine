# Concurrency evidence (database level)

This page records an **actual run** of `tools/sql_concurrency_check.py`. The script executes the same SQL
statements as the Java repositories (`InventoryRepository`, `ReservationRepository`,
`IdempotencyRepository`) from many concurrent PostgreSQL connections, then checks invariants.
Raw output: [`reports/sql-concurrency.json`](../reports/sql-concurrency.json).

**Environment:** PostgreSQL 16.15 (Ubuntu 24.04 package), 2 vCPU container, client and server on the same
host, run on 2026-11-21. Timings are not meaningful as benchmarks and are not quoted.

## Invariants checked

- **I1** `reserved <= total` for every night (no oversell)
- **I2** `reserved` = rooms of reservations currently holding inventory for that night (no leaked/lost rooms)
- **I3** one idempotency key → exactly one reservation under concurrent retries
- **I4** no deadlocks

## Results

| Scenario | Setup | Outcomes | I1 | I2 | I4 |
|---|---|---|---|---|---|
| Contention with cancellations | 32 threads × 50 ops, 10 rooms × 5 nights, 1–3-night stays, 1–2 rooms, 20 % cancels | 146 holds, 1,129 sold out, 112 releases, 213 release no-ops | ✅ | ✅ | ✅ 0 deadlocks |
| Contention without cancellations | same, 0 % cancels | 29 holds, 1,571 sold out; every night ends at exactly 10/10 | ✅ | ✅ | ✅ 0 deadlocks |
| Idempotent retries | 25 concurrent requests, same key | 1 reservation created, 24 replayed, 1 distinct id returned | — | — | — |
| **Negative control**: naive read-then-write (no row locks) | same as row 2 | **356 holds accepted** for 50 room-nights; `reserved` counter never exceeds 10 | ✅ (!) | ❌ mismatch on all 5 nights | — |

Exact counts vary from run to run (thread scheduling); the invariants must hold on every run, and the
script exits non-zero if any fails or if the negative control is *not* detected.

## What the negative control shows

The naive implementation never violated `CHECK (reserved <= total)`: lost updates kept the counter at
10 while 356 reservations were accepted. A database constraint on the counter alone is not enough. The
counter itself becomes wrong. Correctness comes from **serializing the read-check-write with row locks taken
in a deterministic order**; the CHECK constraint is a backstop against code paths that skip locking, and
the I2 audit (also exposed as `GET /api/v1/admin/inventory/audit`) is what detects drift.

## Reproduce

```bash
pip install "psycopg[binary]"
createdb sentinel_check
python3 tools/sql_concurrency_check.py --dsn postgresql://postgres@localhost:5432/sentinel_check --out reports/sql-concurrency.json
```

It also runs in CI (`sql-concurrency-check` job) against a PostgreSQL service container.
