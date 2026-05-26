/*
  One-time cleanup after migrating to per-pipeline medallion schemas.

  Run in DBeaver AFTER:
    cd dbt-warehouse && make run-full

  New layout:
    silver_sales / gold_sales  — retail pipeline
    silver_sap   / gold_sap    — SAP pipeline
    dbt_audit                  — test failures (dev/ci only)

  Legacy schemas (silver, gold) can be dropped once new schemas are populated.
*/

-- Optional: verify new schemas have data before dropping legacy
-- select 'silver_sales' as s, count(*) from silver_sales.stg_orders
-- union all select 'gold_sales', count(*) from gold_sales.fct_orders;

drop schema if exists silver cascade;
drop schema if exists gold cascade;

-- Empty legacy names from earlier PoC iterations
drop view if exists staging.stg_orders;
drop view if exists staging.stg_customers;
drop table if exists analytics.fct_sales_performance;
drop schema if exists staging cascade;
drop schema if exists analytics cascade;
