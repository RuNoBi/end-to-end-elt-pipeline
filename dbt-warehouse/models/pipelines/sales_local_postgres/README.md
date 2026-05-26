# Pipeline: `sales_local_postgres`

Retail sales ELT — `public.customers/orders` → Bronze `src_local_postgres` → **`silver_sales` / `gold_sales`**.

## Lineage

```text
src_local_postgres.customers  →  silver_sales.stg_customers  →  gold_sales.dim_customer
                                                              →  gold_sales.snap_stg_customers
src_local_postgres.orders     →  silver_sales.stg_orders
                                      ↓
                               silver_sales.int_orders_enriched
                                      ↓
                               gold_sales.fct_orders  →  gold_sales.mart_sales_performance
                               gold_sales.dim_date
```

## Run / test

```bash
make run-sales
make snapshot
make test-sales
```

Airflow: `elt_main_pipeline` — `DBT_TARGET=prod` in scheduler.

## CKAN

Published from **`gold_sales.*`** — see `airflow-platform/config/ckan/sales_local_postgres.yaml`.
