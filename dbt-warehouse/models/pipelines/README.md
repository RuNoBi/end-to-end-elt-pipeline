# Pipeline models

Every data product (Airbyte connection + Airflow DAG) owns one folder:

```text
models/pipelines/<pipeline_id>/
├── README.md           ← runbook: sources, models, debug steps
├── staging/            → silver.stg_*     (incremental tables)
│   ├── _sources.yml    ← Bronze + freshness SLA
│   └── _staging.yml    ← Silver tests/docs
├── intermediate/       → silver.int_*     (views)
│   └── _intermediate.yml
└── marts/
    ├── dimensions/     → gold.dim_*
    ├── facts/          → gold.fct_*
    └── marts/          → gold.mart_*
```

## Naming

| Layer | Prefix | Postgres schema |
|-------|--------|-----------------|
| Staging | `stg_` | `silver` |
| Intermediate | `int_` | `silver` (view) |
| Dimension | `dim_` | `gold` |
| Fact | `fct_` | `gold` |
| Mart | `mart_` | `gold` |

Model **names are global** in the project (e.g. `stg_orders`, not `sales__stg_orders`). Tags scope runs: `tag:pipeline_<pipeline_id>`.

## Debug checklist

1. **Which pipeline failed?** — Airflow DAG / tag in log.
2. **Bronze** — `select count(*), max(_airbyte_extracted_at) from <bronze_schema>.<table>`.
3. **Model SQL** — `models/pipelines/<id>/staging/stg_*.sql`.
4. **Compiled SQL** — `target/compiled/de_poc_warehouse/models/pipelines/<id>/...`.
5. **Silver vs Bronze** — compare row counts on `silver.stg_*`.
6. **Gold** — trace `ref()` chain: `stg_*` → `int_*` → `fct_*` / `dim_*` → `mart_*`.
7. **Tests** — failures land in `dbt_audit.*`, not `silver`.

## Commands

```bash
make run-sales    # tag:pipeline_sales_local_postgres
make run-sap      # tag:pipeline_sap_chemicals
make test-silver  # all Silver + singular tests
dbt run --select stg_orders+   # one model and downstream
```

## Pipelines in this repo

| `pipeline_id` | Folder | Airflow DAG | Bronze schema |
|---------------|--------|-------------|---------------|
| `sales_local_postgres` | [sales_local_postgres](./sales_local_postgres/) | `elt_main_pipeline` | `src_local_postgres` |
| `sap_chemicals` | [sap_chemicals](./sap_chemicals/) | `elt_sap_chemicals` | `src_sap_chemicals` |

Snapshots and singular tests for a pipeline live under `snapshots/pipelines/<id>/` and `tests/pipelines/<id>/`.
