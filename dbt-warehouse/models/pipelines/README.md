# Pipeline models

Every data product owns one folder under `models/pipelines/<pipeline_id>/`.

## Schema map (Postgres)

| Pipeline | Silver | Gold | Bronze |
|----------|--------|------|--------|
| [sales_local_postgres](./sales_local_postgres/) | `silver_sales` | `gold_sales` | `src_local_postgres` |
| [sap_chemicals](./sap_chemicals/) | `silver_sap` | `gold_sap` | `src_sap_chemicals` |

Test failures (local dev only): **`dbt_audit`** — not in Silver/Gold.

## Folder pattern

```text
models/pipelines/<pipeline_id>/
├── README.md
├── staging/       → silver_<domain>.stg_*
├── intermediate/  → silver_<domain>.int_*  (views)
└── marts/         → gold_<domain>.dim_* / fct_* / mart_*
```

## Debug checklist

1. Pipeline from Airflow log / tag
2. Bronze row counts + freshness
3. Model SQL in `pipelines/<id>/`
4. Compiled SQL in `target/compiled/.../pipelines/<id>/`
5. Compare `silver_<domain>.stg_*` vs Bronze
6. Trace Gold via `ref()` chain
7. Failed tests → `dbt_audit.*` (when `DBT_TARGET=dev`)

## Run

```bash
make run-sales
dbt run --selector retail_pipeline
dbt test --select tag:pipeline_sap_chemicals
```
