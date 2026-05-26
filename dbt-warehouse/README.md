# dbt Warehouse — Production Medallion Architecture

Docker-native dbt for `data_warehouse`: **Bronze (Airbyte) → Silver (dbt) → Gold (dbt)** with **per-pipeline Postgres schemas**.

## Warehouse layout

```
data_warehouse
├── src_local_postgres / src_sap_chemicals   ← Bronze (Airbyte)
├── silver_sales / silver_sap                ← Silver (stg_* tables, int_* views)
├── gold_sales / gold_sap                    ← Gold (dim_*, fct_*, mart_*)
└── dbt_audit                                ← test failures (dev/ci only)
```

## Pipelines

| Pipeline | Folder | Silver | Gold | DAG |
|----------|--------|--------|------|-----|
| Retail | `models/pipelines/sales_local_postgres/` | `silver_sales` | `gold_sales` | `elt_main_pipeline` |
| SAP | `models/pipelines/sap_chemicals/` | `silver_sap` | `gold_sap` | `elt_sap_chemicals` |

## Quick start

```bash
cp .env.example .env   # DBT_TARGET=dev
make build && make deps
make run-full          # first time or after schema change
make snapshot && make test-silver
```

Airflow uses **`DBT_TARGET=prod`** (no persisted test-failure tables).

## Commands

| Task | Command |
|------|---------|
| All pipelines | `make run` |
| Retail / SAP | `make run-sales` / `make run-sap` |
| Selector | `dbt run --selector retail_pipeline` |
| Lineage + exposures | `make docs-serve` → http://localhost:8081 |
| SQL lint | `make lint` (requires sqlfluff) |

## Consumers

Query **`gold_sales.*`** or **`gold_sap.*`** — not Silver. CKAN publish configs use the same schemas.

```sql
select * from gold_sales.mart_sales_performance order by total_revenue desc limit 20;
select * from gold_sap.mart_sap_chemical_sales_performance limit 20;
```

## Docs

- [docs/MODEL_ORGANIZATION.md](docs/MODEL_ORGANIZATION.md) — conventions, targets, migration
- [docs/PRODUCTION_WORKFLOW.md](docs/PRODUCTION_WORKFLOW.md) — Airflow order
- [models/pipelines/README.md](models/pipelines/README.md) — per-pipeline debug checklist
