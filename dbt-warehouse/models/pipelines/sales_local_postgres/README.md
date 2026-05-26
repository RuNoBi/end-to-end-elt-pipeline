# Pipeline: `sales_local_postgres`

Retail sales ELT — operational Postgres (`public.customers`, `public.orders`) → Airbyte → dbt → CKAN.

## Lineage

```text
src_local_postgres.customers  →  silver.stg_customers  →  gold.dim_customer
                                                          gold.snap_stg_customers (SCD2)
src_local_postgres.orders     →  silver.stg_orders
                                      ↓
                               silver.int_orders_enriched (view)
                                      ↓
                               gold.fct_orders  →  gold.mart_sales_performance
                               gold.dim_date
```

## Where cleaning happens

| Step | File | What it does |
|------|------|----------------|
| Incremental window | `macros/get_raw_incremental_predicate.sql` | Read Bronze with lookback |
| Dedupe | `macros/dedupe_airbyte_change_data.sql` | Latest row per business key |
| Staging | `staging/stg_*.sql` | Cast, trim, rename, null filters |
| Prune deletes | `macros/prune_keys_not_in_bronze.sql` | Post-hook on staging tables |
| Enrich | `intermediate/int_orders_enriched.sql` | Segments, date keys |
| Gold | `marts/**` | Dims, facts, mart aggregate |

## Run / test

```bash
make run-sales
make snapshot          # snap_stg_customers only (this pipeline)
make test-sales
```

Airflow: `elt_main_pipeline` — config `airflow-platform/config/pipelines/sales_local_postgres.yaml`.

## Common issues

| Symptom | Check |
|---------|--------|
| Stale Bronze | `dbt source freshness` / Airbyte sync task |
| Orphan orders | `tests/pipelines/sales_local_postgres/assert_no_orphan_orders_in_silver.sql` |
| Missing customers in dim | `stg_customers` then `dim_customer` incremental window |
| Mart empty | `fct_orders` row count; `dim_customer` join |
