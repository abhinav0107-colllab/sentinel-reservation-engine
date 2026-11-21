-- Independent invariant check after a load run (does not use the application's audit endpoint).
-- Usage: psql -d sentinel -f tools/post_load_check.sql
\echo '1) nights where reserved > total (must be 0 rows):'
SELECT count(*) AS oversold_nights FROM room_inventory WHERE reserved > total;
\echo '2) nights where reserved != rooms held by holding reservations (must be 0):'
WITH held AS (
  SELECT r.room_type_id, d::date AS stay_date, sum(r.rooms) AS rooms
  FROM reservations r, generate_series(r.check_in, r.check_out - 1, interval '1 day') d
  WHERE r.holds_inventory GROUP BY 1, 2)
SELECT count(*) AS drifted_nights
FROM room_inventory i LEFT JOIN held h USING (room_type_id, stay_date)
WHERE i.reserved <> coalesce(h.rooms, 0);
\echo '3) reservation status counts:'
SELECT status, holds_inventory, count(*) FROM reservations GROUP BY 1, 2 ORDER BY 1, 2;
\echo '4) payment status counts:'
SELECT status, count(*) FROM payments GROUP BY 1 ORDER BY 1;
\echo '5) confirmed reservations whose payment is not CAPTURED (must be 0):'
SELECT count(*) FROM reservations r WHERE r.status = 'CONFIRMED'
  AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.reservation_id = r.id AND p.status = 'CAPTURED');
\echo '6) unpublished outbox events:'
SELECT count(*) FROM outbox_events WHERE published_at IS NULL;
