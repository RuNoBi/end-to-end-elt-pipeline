/*
  One-time cleanup after migrating to silver / gold medallion layout.
  Run manually in DBeaver — NOT executed by dbt run.
*/

-- Legacy silver (old schema name "staging")
drop view if exists staging.stg_orders;
drop view if exists staging.stg_customers;

-- Legacy gold (old schema name "analytics")
drop table if exists analytics.fct_sales_performance;

-- Optional: drop empty legacy schemas
-- drop schema if exists staging cascade;
-- drop schema if exists analytics cascade;
