# Production workflow (manual orchestration)

This PoC follows the standard **ingest → transform** split without Airbyte CASCADE.

## Design principles

| Principle | Implementation |
|-----------|----------------|
| Bronze is owned by Airbyte | Schema `src_local_postgres` — do not attach Postgres views |
| Silver is owned by dbt | **Incremental tables** in `silver` — no catalog dependency on Bronze |
| Gold is owned by dbt | `dim_*`, `fct_*`, `mart_*` in schema `gold` |
| Consumers query Gold only | BI / analysts use `gold.*`, not raw |

```
Airbyte  →  src_local_postgres.*     (Bronze, ephemeral from dbt’s perspective)
dbt run  →  silver.* (TABLE)        (conformed copy)
dbt run  →  gold.* (TABLE)           (dimensional + marts)
```

## Manual runbook (no automation yet)

### Daily / after Airbyte sync

```bash
# 1) In Airbyte UI: Sync connection (Destination: drop_cascade = OFF)

# 2) Transform warehouse
cd dbt-warehouse
make run

# 3) Optional quality gate
make test
```

### After large backfill or first-time setup

```bash
cd dbt-warehouse
make run-full
make test
```

### End of day

```bash
# Stop containers only (see root README)
cd ../airbyte-platform && docker compose stop
```

## When to use `run-full`

- First dbt deploy
- Airbyte full refresh / reload of Bronze
- Changed model logic in Silver or Gold
- Tests failing after schema drift

## Airbyte destination settings

| Setting | Production value for this PoC |
|---------|------------------------------|
| Drop tables with CASCADE | **OFF** |
| Namespace / schema | `src_local_postgres` (or your Bronze name) |

Silver tables survive Airbyte table recreation because they are **materialized copies**, not views on Bronze.

## Future automation (not implemented)

Typical next step: Airflow / Dagster / dbt Cloud job with:

```
airbyte_sync_task >> dbt_run_task >> dbt_test_task
```
