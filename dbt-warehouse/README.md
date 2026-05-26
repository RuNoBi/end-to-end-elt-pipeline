# dbt Warehouse — Enterprise Medallion Architecture

Docker-native dbt for `data_warehouse`: **Bronze (Airbyte) → Silver (dbt tables) → Gold (dimensional marts)**.

All transform models live under **`models/pipelines/<pipeline_id>/`** — see [models/pipelines/README.md](models/pipelines/README.md) and [docs/MODEL_ORGANIZATION.md](docs/MODEL_ORGANIZATION.md).

## Warehouse layout

```
data_warehouse
│
├── src_local_postgres     ← Bronze retail (Airbyte)
├── src_sap_chemicals      ← Bronze SAP mock (Airbyte)
│
├── silver                 ← Silver: stg_* tables + int_* views (all pipelines)
├── gold                   ← Gold: dim_* / fct_* / mart_* (all pipelines)
└── dbt_audit              ← Failed test rows only (not business data)
```

## Pipelines

| Pipeline | Folder | DAG |
|----------|--------|-----|
| Retail sales | `models/pipelines/sales_local_postgres/` | `elt_main_pipeline` |
| SAP chemicals | `models/pipelines/sap_chemicals/` | `elt_sap_chemicals` |

## Workflow

**Orchestrated:** Airflow (freshness → Silver → snapshot → tests → Gold).

**Manual:**

```bash
make freshness
make run-sales          # or: make run (both pipelines)
make snapshot           # retail SCD2 only
make test-sales         # or: make test-silver
```

After large Airbyte reload: `make run-full`

Details: [docs/PRODUCTION_WORKFLOW.md](docs/PRODUCTION_WORKFLOW.md)

## Prerequisites

```bash
docker network create de_poc_network 2>/dev/null || true
cd ../warehouse-postgres && docker compose up -d
cd ../dbt-warehouse
cp .env.example .env
make build && make deps
make run-full   # first time
```

## Commands

| Task | Command |
|------|---------|
| All pipelines | `make run` |
| Retail only | `make run-sales` |
| SAP only | `make run-sap` |
| Silver tests | `make test-silver` |
| Full rebuild | `make run-full` |
| Lineage docs | `make docs-serve` → http://localhost:8081 |

## Debug

1. Open `models/pipelines/<pipeline_id>/README.md`
2. Compare Bronze → `silver.stg_*` using compiled SQL in `target/compiled/...`
3. Failed tests: `select * from dbt_audit.<test_table> limit 100;`

## Layer rules

| Layer | Schema | Materialization | Owner |
|-------|--------|-----------------|-------|
| Bronze | `src_*` | Airbyte tables | Airbyte |
| Silver | `silver` | `stg_*` incremental tables; `int_*` views | dbt |
| Gold | `gold` | `dim_*` / `fct_*` incremental; `mart_*` table | dbt |

**Consumers:** query `gold.*` in production.

## Example queries

```sql
select c.customer_name, f.order_amount
from gold.fct_orders f
join gold.dim_customer c using (customer_id)
limit 100;
```
