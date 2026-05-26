# Apache Airflow — ELT Orchestration

Orchestrates **Airbyte Sync → dbt run → dbt test → monitoring** on `de_poc_network`.

| Service | URL |
|---------|-----|
| Airflow UI | [http://localhost:8080](http://localhost:8080) |
| Airbyte UI | [http://localhost:8000](http://localhost:8000) |

## Quick start

```bash
# Prerequisites: de_poc_network, source/warehouse/airbyte stacks running
docker network create de_poc_network 2>/dev/null || true

cd airflow-platform
cp .env.example .env   # edit passwords + Fernet key
mkdir -p logs plugins

export AIRFLOW_UID=50000   # official Airflow Docker UID (do not use host id -u)
mkdir -p logs plugins
sudo chown -R 50000:0 logs plugins 2>/dev/null || true
docker compose build
docker compose up -d
```

Login: credentials from `.env` (`AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD`).

Enable DAG **`elt_main_pipeline`** in the UI (from `config/pipelines/sales_local_postgres.yaml`), then trigger manually or wait for schedule (**11:00 ICT** daily). Add more pipelines via YAML — see [../docs/MULTI_PIPELINE_ARCHITECTURE.md](../docs/MULTI_PIPELINE_ARCHITECTURE.md).

Full setup guide: [docs/AIRFLOW_SETUP.md](docs/AIRFLOW_SETUP.md)

## Architecture

```
airflow-scheduler
    │
    ├─► PythonOperator (OSS Config API)  →  airbyte-proxy:8000/api/v1/connections/sync
    ├─► dbt run/test Silver (gate) → dbt run/test Gold
    └─► Monitor: `log_pipeline_status` (`all_done` — runs on failure too)

**Alerts:** SMTP email on any task failure (after retries) — [docs/AIRFLOW_ALERTING.md](docs/AIRFLOW_ALERTING.md)
    ├─► BashOperator (dbt run/test) →  /opt/dbt (mounted dbt-warehouse/)
    └─► PythonOperator (monitoring logs)
```

## Connection ID

Default Airbyte connection UUID (Variable `airbyte_connection_id`):

`b84b53a7-abfa-4c29-9f6b-c663dd0f4283`

Override in `.env`: `AIRFLOW_VAR_AIRBYTE_CONNECTION_ID=...`
