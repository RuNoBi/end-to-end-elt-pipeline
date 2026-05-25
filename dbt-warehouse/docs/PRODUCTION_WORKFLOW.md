# Production workflow (localhost)

Orchestrated by Airflow DAG **`elt_main_pipeline`**. Manual steps below mirror the same order.

## Automated (recommended)

```text
Trigger DAG elt_main_pipeline in Airflow (http://localhost:8080)
```

```text
1. Airbyte sync          → Bronze (src_local_postgres)
2. dbt source freshness  → fail if Bronze older than SLA
3. dbt run Silver        → silver.stg_* (+ prune deleted keys)
4. dbt snapshot          → gold.snap_stg_customers (SCD2)
5. dbt test Silver       → quality gate
6. dbt run Gold          → dims / facts / marts
7. dbt test Gold
8. Monitor log           → run summary (even on failure)
```

## Manual (same semantics)

```bash
# After Airbyte sync (drop_cascade = OFF)
cd dbt-warehouse
make deps
make freshness
make run
make snapshot
make test-silver
make run    # gold only if silver tests passed — or: --select marts+
make test
```

After large reload or logic change: `make run-full`

## Failure drill

See [../../docs/MONITORING_FAILURE_DRILL.md](../../docs/MONITORING_FAILURE_DRILL.md).

After source revert, rerun from `make run` (or DAG transformation group) — Silver/Gold prune hooks remove stale keys.

## Best-practice reference

[../../docs/BEST_PRACTICES_LOCAL.md](../../docs/BEST_PRACTICES_LOCAL.md)
