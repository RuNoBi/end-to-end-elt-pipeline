# dbt Warehouse — Enterprise Medallion Architecture

Docker-native dbt for `data_warehouse`: **Bronze (Airbyte) → Silver (dbt tables) → Gold (dimensional marts)**.

Production pattern: Silver is **materialized as incremental tables**, not views on Bronze — Airbyte can sync **without CASCADE**.

## Warehouse layout

```
data_warehouse
│
├── src_local_postgres     ← Bronze (Airbyte only — no dbt views)
│   ├── customers
│   └── orders
│
├── silver                 ← Silver (dbt-owned incremental TABLES)
│   ├── stg_customers
│   ├── stg_orders
│   └── int_orders_enriched    (view on silver tables only)
│
└── gold
    ├── dim_customer           (incremental)
    ├── dim_date               (table)
    ├── fct_orders             (incremental)
    └── mart_sales_performance (table, rebuilt each run)
```

## Manual workflow (no orchestrator yet)

See [docs/PRODUCTION_WORKFLOW.md](docs/PRODUCTION_WORKFLOW.md).

```text
1. Airbyte Sync     (drop_cascade = OFF)
2. cd dbt-warehouse && make run
3. make test        (optional)
```

After large Airbyte reload: `make run-full`

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
| Incremental pipeline | `make run` |
| Full rebuild | `make run-full` |
| Tests | `make test` |
| Docs | `make docs-serve` → http://localhost:8081 |

## Airbyte destination

| Setting | Value |
|---------|--------|
| **Drop tables with CASCADE** | **OFF** (production default for this project) |
| Schema | `src_local_postgres` |

If you previously used CASCADE + views, run `make run-full` once after upgrading to table-based Silver.

## Layer rules

| Layer | Schema | Materialization | Owner |
|-------|--------|-----------------|-------|
| Bronze | `src_local_postgres` | Airbyte tables | Airbyte |
| Silver | `silver` | Incremental **tables** | dbt |
| Gold | `gold` | `dim_*` / `fct_*` incremental; `mart_*` table | dbt |

**Consumers:** query `gold.*` only in production.

## Example queries

```sql
select c.customer_name, f.order_amount
from gold.fct_orders f
join gold.dim_customer c using (customer_id)
limit 100;

select * from gold.mart_sales_performance
order by total_revenue desc limit 20;
```

## Performance (~1M rows)

- Silver incremental + indexes on PK / `_airbyte_extracted_at`
- Gold facts incremental; mart full table rebuild (fast aggregate)
- `DBT_THREADS=4`–`8` in `.env`
- See `analyses/performance_strategy.sql`
