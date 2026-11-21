#!/usr/bin/env python3
"""
Database-level concurrency check for Sentinel's reservation SQL.

Runs the *same statements* as ReservationRepository (hold, release, idempotency claim) from many
concurrent connections against a real PostgreSQL and then checks the invariants:

  I1  reserved <= total for every (room_type, night)                       (no oversell)
  I2  reserved == sum(rooms) of reservations holding inventory that night  (no leaked / lost rooms)
  I3  one idempotency key -> exactly one reservation, even under concurrent retries
  I4  no deadlocks (lock ordering is deterministic)

This complements the Java concurrency tests; it isolates the database logic from the application.

  pip install "psycopg[binary]"
  python3 tools/sql_concurrency_check.py --dsn postgresql://postgres@localhost:5432/sentinel_check --out reports/sql-concurrency.json
"""
import argparse
import datetime as dt
import json
import os
import random
import threading
import time
import uuid
from collections import Counter

import psycopg
from psycopg import errors

SCHEMA = os.path.join(os.path.dirname(__file__), "..", "src", "main", "resources", "db", "migration",
                      "V1__inventory_reservations.sql")

# ---- statements mirrored from ReservationRepository / InventoryRepository -----------------------
CLAIM_KEY = """INSERT INTO idempotency_keys (idem_key, operation, request_hash, response_status, response_body)
               VALUES (%s, 'create_reservation', %s, 0, '') ON CONFLICT DO NOTHING"""
LOCK_NIGHTS = """SELECT stay_date, total, reserved, price FROM room_inventory
                 WHERE room_type_id = %s AND stay_date >= %s AND stay_date < %s
                 ORDER BY stay_date FOR UPDATE"""
TAKE = """UPDATE room_inventory SET reserved = reserved + %s, updated_at = now()
          WHERE room_type_id = %s AND stay_date >= %s AND stay_date < %s"""
INSERT_RES = """INSERT INTO reservations (id, hotel_id, room_type_id, check_in, check_out, rooms, guest_name, guest_email,
                                          amount, currency, payment_token, status, hold_expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, 'Load Test', 'load@test.invalid', %s, 'USD', 'tok_ok',
                        'PENDING_PAYMENT', now() + interval '10 minutes')"""
COMPLETE_KEY = """UPDATE idempotency_keys SET response_status = 201, response_body = %s
                  WHERE operation = 'create_reservation' AND idem_key = %s"""
RELEASE = """UPDATE reservations SET status = %s, holds_inventory = FALSE, version = version + 1, updated_at = now()
             WHERE id = %s AND holds_inventory
             RETURNING room_type_id, check_in, check_out, rooms"""
GIVE_BACK = """UPDATE room_inventory SET reserved = reserved - %s, updated_at = now()
               WHERE room_type_id = %s AND stay_date >= %s AND stay_date < %s"""


class SoldOut(Exception):
    pass


def hold(conn, hotel_id, room_type_id, check_in, check_out, rooms, idem_key):
    """Returns (reservation_id, replayed)."""
    with conn.transaction():
        cur = conn.cursor()
        cur.execute(CLAIM_KEY, (idem_key, "h" * 64))
        if cur.rowcount == 0:  # a committed request with this key exists -> replay
            cur.execute("SELECT response_body FROM idempotency_keys WHERE operation='create_reservation' AND idem_key=%s",
                        (idem_key,))
            return json.loads(cur.fetchone()[0])["id"], True
        nights = (check_out - check_in).days
        rows = cur.execute(LOCK_NIGHTS, (room_type_id, check_in, check_out)).fetchall()
        if len(rows) != nights or any(total - reserved < rooms for _, total, reserved, _ in rows):
            raise SoldOut()
        cur.execute(TAKE, (rooms, room_type_id, check_in, check_out))
        rid = uuid.uuid4()
        amount = sum(p for *_, p in rows) * rooms
        cur.execute(INSERT_RES, (rid, hotel_id, room_type_id, check_in, check_out, rooms, amount))
        cur.execute(COMPLETE_KEY, (json.dumps({"id": str(rid)}), idem_key))
        return str(rid), False


def naive_hold(conn, hotel_id, room_type_id, check_in, check_out, rooms, idem_key):
    """NEGATIVE CONTROL ONLY: read-then-write without row locks (the classic lost-update bug).
    Used to prove the checker can detect broken concurrency control; never used by the application."""
    with conn.transaction():
        cur = conn.cursor()
        nights = (check_out - check_in).days
        rows = cur.execute(LOCK_NIGHTS.replace(" FOR UPDATE", ""), (room_type_id, check_in, check_out)).fetchall()
        if len(rows) != nights or any(total - reserved < rooms for _, total, reserved, _ in rows):
            raise SoldOut()
        time.sleep(0.001)  # widen the race window, as a GC pause or slow network would
        for stay_date, _, reserved, _ in rows:
            cur.execute("UPDATE room_inventory SET reserved = %s WHERE room_type_id = %s AND stay_date = %s",
                        (reserved + rooms, room_type_id, stay_date))
        rid = uuid.uuid4()
        cur.execute(INSERT_RES, (rid, hotel_id, room_type_id, check_in, check_out, rooms, 100))
        return str(rid), False


def release(conn, reservation_id, new_status="CANCELLED"):
    with conn.transaction():
        cur = conn.cursor()
        row = cur.execute(RELEASE, (new_status, reservation_id)).fetchone()
        if row is None:
            return False  # already released by someone else: idempotent
        rt, ci, co, rooms = row
        cur.execute(LOCK_NIGHTS, (rt, ci, co)).fetchall()
        cur.execute(GIVE_BACK, (rooms, rt, ci, co))
        return True


def setup(dsn, total, nights, start):
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        c.execute(open(SCHEMA).read())
        hotel, rt = uuid.uuid4(), uuid.uuid4()
        c.execute("INSERT INTO hotels (id, name) VALUES (%s, 'Concurrency Hotel')", (hotel,))
        c.execute("INSERT INTO room_types (id, hotel_id, name, max_guests) VALUES (%s, %s, 'Deluxe', 2)", (rt, hotel))
        for i in range(nights):
            c.execute("INSERT INTO room_inventory (room_type_id, stay_date, total, price, currency) VALUES (%s,%s,%s,100,'USD')",
                      (rt, start + dt.timedelta(days=i), total))
        return hotel, rt


def check_invariants(dsn, rt):
    with psycopg.connect(dsn) as c:
        inv = c.execute("SELECT stay_date, total, reserved FROM room_inventory WHERE room_type_id=%s ORDER BY stay_date",
                        (rt,)).fetchall()
        expected = dict(c.execute("""
            SELECT d::date, coalesce(sum(r.rooms), 0)
            FROM room_inventory i
            CROSS JOIN LATERAL (SELECT i.stay_date AS d) x
            LEFT JOIN reservations r ON r.room_type_id = i.room_type_id AND r.holds_inventory
                                    AND i.stay_date >= r.check_in AND i.stay_date < r.check_out
            WHERE i.room_type_id = %s GROUP BY d ORDER BY d""", (rt,)).fetchall())
    oversold = [str(d) for d, total, reserved in inv if reserved > total]
    mismatch = [str(d) for d, _, reserved in inv if reserved != expected.get(d, 0)]
    return {"nights": [{"date": str(d), "total": t, "reserved": r} for d, t, r in inv],
            "oversold_nights": oversold, "accounting_mismatches": mismatch}


def contention_scenario(dsn, threads, attempts_per_thread, total, nights, cancel_ratio, seed, hold_fn=None):
    hold_fn = hold_fn or hold
    start = dt.date(2027, 3, 1)
    hotel, rt = setup(dsn, total, nights, start)
    outcomes = Counter()
    held = []
    lock = threading.Lock()
    barrier = threading.Barrier(threads)

    def worker(wid):
        rng = random.Random(seed * 100 + wid)
        with psycopg.connect(dsn) as conn:
            barrier.wait()
            for _ in range(attempts_per_thread):
                if held and rng.random() < cancel_ratio:
                    with lock:
                        rid = rng.choice(held) if held else None
                    if rid:
                        try:
                            ok = release(conn, rid)
                            with lock:
                                outcomes["released" if ok else "release_noop"] += 1
                        except errors.DeadlockDetected:
                            with lock:
                                outcomes["deadlock"] += 1
                    continue
                ci = start + dt.timedelta(days=rng.randrange(nights))
                co = min(start + dt.timedelta(days=nights), ci + dt.timedelta(days=rng.randint(1, 3)))
                try:
                    rid, _ = hold_fn(conn, hotel, rt, ci, co, rng.randint(1, 2), str(uuid.uuid4()))
                    with lock:
                        outcomes["held"] += 1
                        held.append(rid)
                except SoldOut:
                    with lock:
                        outcomes["sold_out"] += 1
                except errors.DeadlockDetected:
                    with lock:
                        outcomes["deadlock"] += 1
                except errors.CheckViolation:
                    with lock:
                        outcomes["check_violation"] += 1

    t0 = time.time()
    ts = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    elapsed = time.time() - t0
    inv = check_invariants(dsn, rt)
    return {"threads": threads, "attempts": threads * attempts_per_thread, "rooms_per_night": total,
            "nights": nights, "cancel_ratio": cancel_ratio, "elapsed_s": round(elapsed, 2),
            "outcomes": dict(outcomes), **inv}


def idempotency_scenario(dsn, threads):
    start = dt.date(2027, 4, 1)
    hotel, rt = setup(dsn, 5, 3, start)
    key = "retry-" + str(uuid.uuid4())
    results, barrier, lock = [], threading.Barrier(threads), threading.Lock()

    def worker():
        with psycopg.connect(dsn) as conn:
            barrier.wait()
            r = hold(conn, hotel, rt, start, start + dt.timedelta(days=2), 1, key)
            with lock:
                results.append(r)

    ts = [threading.Thread(target=worker) for _ in range(threads)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    with psycopg.connect(dsn) as c:
        n = c.execute("SELECT count(*) FROM reservations").fetchone()[0]
        reserved = c.execute("SELECT max(reserved) FROM room_inventory").fetchone()[0]
    return {"concurrent_requests_same_key": threads, "reservations_created": n,
            "distinct_ids_returned": len({r[0] for r in results}), "replayed": sum(1 for r in results if r[1]),
            "max_reserved": reserved}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="postgresql://postgres@localhost:5432/sentinel_check")
    ap.add_argument("--threads", type=int, default=32)
    ap.add_argument("--attempts", type=int, default=50)
    ap.add_argument("--rooms", type=int, default=10)
    ap.add_argument("--nights", type=int, default=5)
    ap.add_argument("--cancel-ratio", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out")
    a = ap.parse_args()
    with psycopg.connect(a.dsn) as c:
        pg = c.execute("SHOW server_version").fetchone()[0]
    report = {"kind": "sql-concurrency-check", "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
              "postgres_version": pg, "cpu_count": os.cpu_count(),
              "contention": contention_scenario(a.dsn, a.threads, a.attempts, a.rooms, a.nights, a.cancel_ratio, a.seed),
              "no_cancellation": contention_scenario(a.dsn, a.threads, a.attempts, a.rooms, a.nights, 0.0, a.seed + 1),
              "idempotency": idempotency_scenario(a.dsn, 25),
              # Negative control: the same checker applied to deliberately broken code must detect a violation.
              "negative_control_naive": contention_scenario(a.dsn, a.threads, a.attempts, a.rooms, a.nights, 0.0,
                                                            a.seed + 2, hold_fn=naive_hold)}
    c1, c2, idem = report["contention"], report["no_cancellation"], report["idempotency"]
    passed = (not c1["oversold_nights"] and not c1["accounting_mismatches"] and not c2["oversold_nights"]
              and not c2["accounting_mismatches"] and c1["outcomes"].get("deadlock", 0) == 0
              and c2["outcomes"].get("deadlock", 0) == 0 and c1["outcomes"].get("check_violation", 0) == 0
              and idem["reservations_created"] == 1 and idem["distinct_ids_returned"] == 1
              and all(n["reserved"] == n["total"] for n in c2["nights"]))
    neg = report["negative_control_naive"]
    report["negative_control_detected_violation"] = bool(neg["oversold_nights"] or neg["accounting_mismatches"]
                                                         or neg["outcomes"].get("check_violation", 0))
    passed = passed and report["negative_control_detected_violation"]
    report["invariants_hold"] = passed
    print(json.dumps(report, indent=2, default=str))
    if a.out:
        os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
        json.dump(report, open(a.out, "w"), indent=2, default=str)
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
