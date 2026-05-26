# Credential map (local PoC)

Use the same warehouse password everywhere marked **warehouse**.

| Role | Variable(s) | Files | Docker hostname (internal) | Host (DBeaver) |
|------|-------------|-------|--------------------------|----------------|
| Source DB | `POSTGRES_*` | `source-postgres/.env` | `de_poc_source_postgres:5432` | `localhost:5433` |
| Warehouse DB | `POSTGRES_*` | `warehouse-postgres/.env` | `de_poc_warehouse_postgres:5432` | `localhost:5434` |
| dbt → warehouse | `DBT_WAREHOUSE_*` | `dbt-warehouse/.env` | `de_poc_warehouse_postgres:5432` | — |
| Airflow metadata | `POSTGRES_*` | `airflow-platform/.env` | `airflow-postgres:5432` | — |
| Airflow → dbt | `DBT_WAREHOUSE_*` | `airflow-platform/.env` | same as warehouse | — |
| Airbyte API (Airflow) | Variables `airbyte_connection_id`, `airbyte_api_base_url` | `airflow-platform/.env` | `http://airbyte-proxy:8000/api/v1` | `localhost:8000` |
| Airbyte sync job | `AIRFLOW_VAR_AIRBYTE_CONNECTION_ID` | `airflow-platform/.env` | UUID in Airbyte UI | — |
| CKAN catalog | `CKAN_*`, `AIRFLOW_VAR_CKAN_*` | `ckan-platform/.env`, `airflow-platform/.env` | `http://ckan:5000` | http://localhost:5001 |
| CKAN admin UI | `CKAN_SYSADMIN_*` | `ckan-platform/.env` | — | http://localhost:5001 |

**CKAN API token:** run `ckan-platform/scripts/bootstrap-ckan.sh` — token is written to local `.env` only (never commit).

**Airbyte destination (UI):** `drop_cascade` = **OFF**
