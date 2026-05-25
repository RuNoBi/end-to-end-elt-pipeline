-- Remove drill rows left in warehouse after source revert.
-- Bronze is updated by the next Airbyte sync; Silver incremental tables keep stale keys until deleted.

BEGIN;

DELETE FROM silver.stg_orders
WHERE order_id >= 900000000;

DELETE FROM silver.stg_customers
WHERE customer_id >= 900000000;

-- Gold incremental tables also retain deleted upstream keys until removed
DELETE FROM gold.fct_orders
WHERE order_id >= 900000000;

DELETE FROM gold.mart_sales_performance
WHERE customer_id >= 900000000;

DELETE FROM src_local_postgres.orders
WHERE id >= 900000000
   OR status LIKE 'BAD_DATA_DRILL%';

DELETE FROM src_local_postgres.customers
WHERE id >= 900000000;

COMMIT;
