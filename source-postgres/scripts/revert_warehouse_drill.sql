-- Remove drill rows left in warehouse after source revert.
-- Schemas follow per-pipeline medallion layout (see dbt_project.yml vars).

BEGIN;

DELETE FROM silver_sales.stg_orders
WHERE order_id >= 900000000;

DELETE FROM silver_sales.stg_customers
WHERE customer_id >= 900000000;

DELETE FROM gold_sales.fct_orders
WHERE order_id >= 900000000;

DELETE FROM gold_sales.mart_sales_performance
WHERE customer_id >= 900000000;

DELETE FROM src_local_postgres.orders
WHERE id >= 900000000
   OR status LIKE 'BAD_DATA_DRILL%';

DELETE FROM src_local_postgres.customers
WHERE id >= 900000000;

COMMIT;
