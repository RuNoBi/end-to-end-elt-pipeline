/*
  Performance playbook for ~1M order rows in data_warehouse.
  Run manually via psql on the warehouse (not executed by dbt run).
*/

-- 1) Planner statistics after large loads
analyze src_local_postgres.orders;
analyze src_local_postgres.customers;
analyze gold.fct_orders;
analyze gold.dim_customer;
analyze gold.mart_sales_performance;

-- 2) B-tree indexes on raw landing (helps staging views at scale)
create index if not exists idx_raw_orders_customer_id
    on src_local_postgres.orders (customer_id);

create index if not exists idx_raw_orders_order_date
    on src_local_postgres.orders (order_date);

-- 3) BRIN index on time — compact and fast for append-heavy fact loads
create index if not exists idx_raw_orders_order_date_brin
    on src_local_postgres.orders
    using brin (order_date);

-- 4) Optional: partition raw orders by month (advanced; requires table rebuild)
-- create table src_local_postgres.orders_partitioned ( ... ) partition by range (order_date);
