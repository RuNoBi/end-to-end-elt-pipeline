# Production readiness checklist (ELT + CKAN)

Use this after PoC tuning. Company-specific passwords and URLs go in `.env` files (not committed).

## Airbyte (Bronze)

- [ ] Connection **source → dwh** streams: `incremental` + `append_dedup`
- [ ] Cursor fields: `customers.created_at`, `orders.order_date`
- [ ] Destination namespace: `src_local_postgres` (matches dbt `sources`)
- [ ] **Do not** use full_refresh + overwrite on large tables in production
- [ ] Schedule: Airflow DAG triggers sync (`scheduleType: manual` in Airbyte is OK)
- [ ] `AIRFLOW_VAR_AIRBYTE_CONNECTION_ID` set in `airflow-platform/.env`

Verify locally:

```bash
./scripts/verify-pipeline-ready.sh
```

The DAG runs `validate_airbyte_connection` before every sync and fails if modes are wrong.

## Airflow

- [ ] DAG `elt_main_pipeline` enabled; `max_active_runs=1`
- [ ] Warehouse credentials in `.env` match `warehouse-postgres` and `dbt-warehouse`
- [ ] No `AIRBYTE_SKIP_*` variables (removed — always sync)
- [ ] Daily ops: `scripts/daily-start.sh` / `daily-stop.sh` (avoid `docker compose down` on `ckan-db`)

## dbt

- [ ] `dbt source freshness` SLA: warn 24h / error 72h on `_airbyte_extracted_at`
- [ ] Silver tests pass before Gold (`dbt_test_silver` in DAG)
- [ ] CI: `.github/workflows/quality-gates.yml` (Postgres service + compile)

## CKAN publication

- [ ] Run `ckan-platform/scripts/bootstrap-ckan.sh` after first CKAN start or DB reset
- [ ] Copy token to `airflow-platform/.env`: `CKAN_API_TOKEN`, `AIRFLOW_VAR_CKAN_API_TOKEN`
- [ ] `CKAN_URL=http://ckan:5000` from Airflow containers
- [ ] Row caps: `CKAN_PUBLISH_MAX_ROWS` (default 100k mart), dim 50k in code

## What you change per environment

| Area | PoC / local | Production |
|------|-------------|------------|
| Passwords | `.env` examples | Company secrets manager |
| `CKAN_URL` | `http://ckan:5000` | Internal service URL |
| SMTP / alerts | Mailpit | Company mail relay |
| Airbyte / Postgres hosts | Docker network names | Managed service endpoints |

## End-to-end test

1. `./scripts/daily-start.sh` (or start stacks manually)
2. `./scripts/verify-pipeline-ready.sh`
3. Airflow UI → trigger `elt_main_pipeline`
4. CKAN UI → organization **UBE Group Thailand** → open mart datasets → Data Explorer
