-- Remove drill rows injected for failure simulation.
BEGIN;

DELETE FROM public.orders
WHERE id >= 900000000
   OR status LIKE 'BAD_DATA_DRILL%';

DELETE FROM public.customers
WHERE id >= 900000000;

COMMIT;

SELECT 'revert_ok' AS result;
