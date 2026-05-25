-- Optional: bulk load ~1M orders for performance demos (run manually, not on first init).
-- Usage: docker exec -i de_poc_source_postgres psql -U source_admin -d sales_source < scripts/seed-orders-1m.sql

INSERT INTO public.orders (customer_id, amount, order_date, status)
SELECT
    1 + (floor(random() * 20))::int,
    round((random() * 500 + 10)::numeric, 2),
    NOW() - (floor(random() * 365))::int * interval '1 day',
    CASE WHEN random() < 0.05 THEN 'cancelled' ELSE 'completed' END
FROM generate_series(1, 1000000);

ANALYZE public.orders;
