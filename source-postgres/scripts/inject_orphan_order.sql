BEGIN;

INSERT INTO public.orders (id, customer_id, amount, order_date, status)
OVERRIDING SYSTEM VALUE
VALUES (900000001, 999999999, 123.45, NOW(), 'BAD_DATA_DRILL_ORPHAN')
ON CONFLICT (id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    amount = EXCLUDED.amount,
    order_date = EXCLUDED.order_date,
    status = EXCLUDED.status;

COMMIT;
