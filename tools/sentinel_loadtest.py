#!/usr/bin/env python3
"""
Reservation load generator for a running Sentinel (standard library only).

Scenarios
  flash-sale : many clients compete for few rooms on the same dates (contention, correctness under load)
  spread     : traffic spread over many room types and dates (throughput with low contention)

  python3 tools/sentinel_loadtest.py --scenario flash-sale --rooms 50 --concurrency 32 --duration 30 --out reports/flash.json
  python3 tools/sentinel_loadtest.py --scenario spread --room-types 200 --concurrency 32 --duration 60 --out reports/spread.json

After the run it waits for the payment saga to settle and calls the inventory audit, so every report
states whether the "never oversell / no drift" invariant held under that load. Nothing is estimated:
all numbers are computed from responses received during the run.
"""
import argparse
import datetime as dt
import http.client
import json
import math
import os
import platform
import random
import statistics
import threading
import time
import urllib.parse
import uuid


class Client:
    def __init__(self, base, key=None):
        u = urllib.parse.urlparse(base)
        self.host, self.port, self.key, self.conn = u.hostname, u.port or 80, key, None

    def req(self, method, path, body=None, headers=None):
        h = {"Accept": "application/json", **(headers or {})}
        data = None
        if body is not None:
            data, h["Content-Type"] = json.dumps(body).encode(), "application/json"
        if self.key:
            h["X-Api-Key"] = self.key
        try:
            if self.conn is None:
                self.conn = http.client.HTTPConnection(self.host, self.port, timeout=15)
            self.conn.request(method, path, body=data, headers=h)
            r = self.conn.getresponse()
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
        except (OSError, http.client.HTTPException):
            if self.conn:
                self.conn.close()
            self.conn = None
            raise


def setup(admin, room_types, rooms, start, nights):
    ids = []
    for i in range(room_types):
        s, body = admin.req("POST", "/api/v1/admin/hotels",
                            {"name": f"Load Hotel {i} {uuid.uuid4().hex[:6]}", "roomTypes": [{"name": "Std", "maxGuests": 2}]})
        assert s == 201, (s, body)
        rt = body["roomTypes"][0]["roomTypeId"]
        s, body = admin.req("PUT", f"/api/v1/admin/room-types/{rt}/inventory",
                            {"from": str(start), "to": str(start + dt.timedelta(days=nights)), "total": rooms,
                             "price": "100.00", "currency": "USD"})
        assert s == 200, (s, body)
        ids.append(rt)
    return ids


def pct(sorted_vals, p):
    if not sorted_vals:
        return None
    return round(sorted_vals[min(len(sorted_vals) - 1, max(0, math.ceil(p / 100 * len(sorted_vals)) - 1))], 2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://localhost:8081")
    ap.add_argument("--api-key", default=os.environ.get("SENTINEL_ADMIN_API_KEY"))
    ap.add_argument("--scenario", choices=["flash-sale", "spread"], default="flash-sale")
    ap.add_argument("--room-types", type=int, default=1)
    ap.add_argument("--rooms", type=int, default=50)
    ap.add_argument("--nights", type=int, default=14)
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--duration", type=int, default=30)
    ap.add_argument("--decline-ratio", type=float, default=0.1)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--out")
    a = ap.parse_args()
    if not a.api_key:
        raise SystemExit("--api-key or SENTINEL_ADMIN_API_KEY required")

    admin = Client(a.base_url, a.api_key)
    start = dt.date.today() + dt.timedelta(days=60)
    rts = setup(admin, a.room_types if a.scenario == "spread" else 1, a.rooms, start, a.nights)

    lock = threading.Lock()
    lat, codes = [], {}
    stop = time.time() + a.duration

    def worker(wid):
        rng = random.Random(a.seed * 1000 + wid)
        c = Client(a.base_url)
        my_lat, my_codes = [], {}
        while time.time() < stop:
            rt = rng.choice(rts)
            if a.scenario == "flash-sale":
                ci, n = start, 2
            else:
                ci, n = start + dt.timedelta(days=rng.randrange(a.nights - 3)), rng.randint(1, 3)
            body = {"roomTypeId": rt, "checkIn": str(ci), "checkOut": str(ci + dt.timedelta(days=n)), "rooms": 1,
                    "guestName": "Load", "guestEmail": "load@example.com",
                    "paymentToken": "tok_decline" if rng.random() < a.decline_ratio else "tok_ok"}
            t = time.perf_counter()
            try:
                s, _ = c.req("POST", "/api/v1/reservations", body, {"Idempotency-Key": str(uuid.uuid4())})
            except Exception:
                s = -1
            my_lat.append((time.perf_counter() - t) * 1000)
            my_codes[s] = my_codes.get(s, 0) + 1
        with lock:
            lat.extend(my_lat)
            for k, v in my_codes.items():
                codes[k] = codes.get(k, 0) + v

    t0 = time.time()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(a.concurrency)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    elapsed = time.time() - t0
    lat.sort()

    # Let the saga settle (payments, compensations), then audit invariants.
    audit = None
    for _ in range(60):
        s, audit = admin.req("GET", "/api/v1/admin/inventory/audit")
        if s == 200 and audit.get("consistent"):
            break
        time.sleep(1)

    total = len(lat)
    unexpected = sum(v for k, v in codes.items() if k not in (201, 409))
    report = {
        "kind": "sentinel-load-test", "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "environment": {"cpu_count": os.cpu_count(), "os": platform.platform(), "base_url": a.base_url},
        "parameters": vars(a) | {"api_key": "<redacted>"},
        "results": {
            "requests": total, "elapsed_s": round(elapsed, 2), "throughput_rps": round(total / elapsed, 1),
            "latency_ms": {"p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99), "max": pct(lat, 100),
                           "mean": round(statistics.mean(lat), 2) if lat else None},
            "status_counts": {str(k): v for k, v in sorted(codes.items())},
            "unexpected_error_rate": round(unexpected / total, 5) if total else None,
            "inventory_audit_consistent": bool(audit and audit.get("consistent")),
        },
    }
    print(json.dumps(report["results"], indent=2))
    if a.out:
        os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
        json.dump(report, open(a.out, "w"), indent=2)


if __name__ == "__main__":
    main()
