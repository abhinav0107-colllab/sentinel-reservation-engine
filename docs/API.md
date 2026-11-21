# Sentinel API

Base URL (local): `http://localhost:8081`. Admin endpoints require `X-Api-Key: <SENTINEL_ADMIN_API_KEY>`.
Errors are RFC 7807 with a stable `type`, e.g. `urn:sentinel:error:sold-out`.

## Admin: hotels and inventory

```bash
curl -s -X POST localhost:8081/api/v1/admin/hotels -H "X-Api-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"name":"Riverside Grand","roomTypes":[{"name":"Deluxe King","maxGuests":2}]}'
# 201 {"hotelId":"…","roomTypes":[{"roomTypeId":"…","name":"Deluxe King"}]}

curl -s -X PUT localhost:8081/api/v1/admin/room-types/$RT/inventory -H "X-Api-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"from":"2027-03-01","to":"2027-04-01","total":20,"price":"150.00","currency":"USD"}'
# 200 {"nightsUpdated":31}    409 if total would drop below rooms already reserved

curl -s localhost:8081/api/v1/admin/inventory/audit -H "X-Api-Key: $KEY"
# {"consistent":true,"violations":[]}
```

## Availability (public, lock-free)

```
GET /api/v1/availability?roomTypeId=…&checkIn=2027-03-01&checkOut=2027-03-03
→ {"availableRooms": 18, "pricePerRoom": 300.00, "currency": "USD", "nights": [...]}
```

Availability is a snapshot; booking re-validates under row locks.

## Create a reservation

```bash
curl -s -i -X POST localhost:8081/api/v1/reservations \
  -H 'Idempotency-Key: 3f1c2b0e-7a7e-4c9e-9d1f-2b6f0c1d9a11' -H 'Content-Type: application/json' -d '{
  "roomTypeId": "…", "checkIn": "2027-03-01", "checkOut": "2027-03-03", "rooms": 1,
  "guestName": "Ada Lovelace", "guestEmail": "ada@example.com", "paymentToken": "tok_ok"
}'
```

| Status | Meaning |
|---|---|
| 201 | Hold created: `{"id", "status": "PENDING_PAYMENT", "amount", "holdExpiresAt", …}`. Payment completes asynchronously; poll `GET /api/v1/reservations/{id}` |
| 201 + `Idempotent-Replayed: true` | Retry of an already-committed request; same body as the original |
| 400 | Missing/invalid `Idempotency-Key` or body |
| 404 | Unknown room type |
| 409 `sold-out` | At least one night lacks capacity (retrying with the same key is allowed) |
| 422 `idempotency-key-reuse` | Key already used with a different body |

### Simulated payment tokens

| Token | Behaviour |
|---|---|
| `tok_ok` (or any other `tok_*`) | Captured → `CONFIRMED` |
| `tok_decline` | Declined → `PAYMENT_FAILED`, rooms released |
| `tok_timeout_after_capture` | First call charges then times out; retry returns the original charge → `CONFIRMED`, charged once |
| `tok_provider_down` | Always times out → stays `PENDING_PAYMENT` until the hold expires → `EXPIRED` |

## Read / cancel

```
GET  /api/v1/reservations/{id}
POST /api/v1/reservations/{id}/cancel     → 200 CANCELLED (idempotent); 409 if EXPIRED/PAYMENT_FAILED
```

Cancelling a confirmed reservation releases the rooms immediately and triggers an asynchronous refund.
