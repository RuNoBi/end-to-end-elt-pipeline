# Airflow setup guide — Airbyte + dbt ELT

Step-by-step instructions to wire Airflow to your existing Docker stack.

**Failure email alerts:** see [AIRFLOW_ALERTING.md](./AIRFLOW_ALERTING.md) (SMTP + `6733193821@student.chula.ac.th` or your on-call list).

## Prerequisites

1. Shared network exists:

   ```bash
   docker network create de_poc_network
   ```

2. These stacks are up:

   ```bash
   cd source-postgres && docker compose up -d
   cd ../warehouse-postgres && docker compose up -d
   cd ../airbyte-platform && docker compose up -d
   ```

3. Airbyte connection **`source -> dwh`** exists with ID:

   ```
   b84b53a7-abfa-4c29-9f6b-c663dd0f4283
   ```

   Verify in Airbyte UI: **Connections → source -> dwh → Settings** (connection UUID in URL or settings).

4. dbt project works standalone:

   ```bash
   cd dbt-warehouse && make run && make test
   ```

---

## Step 1 — Configure Airflow environment

```bash
cd airflow-platform
cp .env.example .env
```

Edit `.env`:

| Variable | Purpose |
|----------|---------|
| `AIRFLOW__CORE__FERNET_KEY` | Generate: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `AIRFLOW_ADMIN_*` | Web UI login |
| `DBT_WAREHOUSE_*` | Must match `warehouse-postgres/.env` |
| `AIRFLOW_VAR_AIRBYTE_CONNECTION_ID` | Your Airbyte connection UUID |

---

## Step 2 — Start Airflow

```bash
export AIRFLOW_UID=50000
mkdir -p logs plugins
sudo chown -R 50000:0 logs plugins 2>/dev/null || true
docker compose build
docker compose up -d
docker compose ps
```

Open [http://localhost:8080](http://localhost:8080) and log in.

---

## Step 3 — Airbyte variables (no Airflow connection required)

The DAG calls the **Airbyte OSS 0.63 Config API** directly (`POST /api/v1/connections/sync`).  
The bundled `AirbyteTriggerSyncOperator` targets the newer Cloud `/jobs` API and fails on OSS with `Invalid URL 'airbyte-proxy/jobs'`.

Set these in `airflow-platform/.env` (loaded on container start):

| Variable | Example |
|----------|---------|
| `AIRFLOW_VAR_AIRBYTE_CONNECTION_ID` | `b84b53a7-abfa-4c29-9f6b-c663dd0f4283` |
| `AIRFLOW_VAR_AIRBYTE_API_BASE_URL` | `http://airbyte-proxy:8000/api/v1` |

Optional check from the scheduler container:

```bash
docker compose exec airflow-scheduler python3 -c "
import requests
r = requests.post('http://airbyte-proxy:8000/api/v1/connections/sync',
    json={'connectionId': 'YOUR_CONNECTION_UUID'}, timeout=30)
print(r.status_code, r.json().get('job', {}).get('id'))
"
```

### Why these values?

- From inside Airflow containers, Airbyte API base URL is:
  `http://airbyte-proxy:8000/api/v1/`
- Sync endpoint used by the provider: `POST /connections/sync`
- Verified locally with connection ID `b84b53a7-abfa-4c29-9f6b-c663dd0f4283`

---

## Step 4 — Set the Airbyte Connection ID (Variable)

The DAG reads Variable **`airbyte_connection_id`**.

**Option A — via `.env` (recommended, already in compose):**

```env
AIRFLOW_VAR_AIRBYTE_CONNECTION_ID=b84b53a7-abfa-4c29-9f6b-c663dd0f4283
```

Restart scheduler after changing:

```bash
docker compose restart airflow-scheduler airflow-webserver
```

**Option B — via Airflow UI:**

1. **Admin → Variables → +**
2. Key: `airbyte_connection_id`
3. Val: `b84b53a7-abfa-4c29-9f6b-c663dd0f4283`

---

## Step 5 — Enable and run the DAG

1. **DAGs → `elt_main_pipeline`**
2. Toggle **Unpause**
3. Click **Trigger DAG** (play icon)

### Expected task groups (UI)

```
extraction
  └── trigger_airbyte_sync
transformation
  ├── dbt_run
  └── dbt_test
monitoring
  └── log_pipeline_summary
```

Dependencies: `extraction >> transformation >> monitoring`, and `dbt_run >> dbt_test`.

---

## Step 6 — Verify end-to-end

```bash
# Airbyte job logs in UI: Connections → source -> dwh → Job history

# Warehouse row counts (DBeaver / psql)
docker exec de_poc_warehouse_postgres psql -U warehouse_admin -d data_warehouse -c \
  "SELECT COUNT(*) FROM gold.fct_orders;"

# Airflow task logs: UI → DAG → Graph → Task → Log
```

---

## Retries & timeouts

Configured in `dags/elt_main_pipeline.py`:

| Setting | Value |
|---------|--------|
| `retries` | 2 |
| `retry_delay` | 5 minutes |
| Airbyte `timeout` | 7200 seconds (2 hours) |
| `execution_timeout` | 2 hours per task |

---

## Optional: Slack / email on failure

### Slack (webhook)

1. Create Slack incoming webhook
2. Airflow UI → **Admin → Connections → +**
   - Connection Id: `slack_default`
   - Type: `HTTP`
   - Host: `hooks.slack.com`
   - Password: `/services/YOUR/WEBHOOK/PATH` *(as token path)*

   Or use `SlackWebhookOperator` in a separate `on_failure_callback` DAG.

3. Add to DAG `default_args`:

   ```python
   from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification

   default_args = {
       ...
       "on_failure_callback": [
           send_slack_webhook_notification(
               slack_webhook_conn_id="slack_default",
               text="ELT pipeline failed: {{ dag.dag_id }} / {{ ti.task_id }}",
           )
       ],
   }
   ```

### Email (SMTP)

Add to `airflow-platform/.env`:

```env
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=your_email@example.com
AIRFLOW__SMTP__SMTP_PASSWORD=your_app_password
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_MAIL_FROM=your_email@example.com
```

Set in DAG:

```python
default_args = {
    "email": ["data-team@example.com"],
    "email_on_failure": True,
}
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Airbyte task 404 / connection refused | Ensure `airbyte-proxy` is on `de_poc_network`; use host `airbyte-proxy`, port `8000` |
| dbt command not found | Rebuild image: `docker compose build --no-cache` |
| dbt auth failed | Align `DBT_WAREHOUSE_*` in `airflow-platform/.env` with warehouse Postgres |
| Permission denied on logs/ | `chmod -R 775 logs` or use Docker volume `airflow_logs` (default in compose) |
| DAG runs stuck **queued** | Another run is **running** (`max_active_runs=1`). Fail stuck runs: see RUN_STEP_BY_STEP.md troubleshooting |
| Scheduler **unhealthy** | `docker compose restart airflow-scheduler` after fixing logs |
| DAG not visible | Check `./dags/elt_main_pipeline.py` syntax; `docker compose logs airflow-scheduler` |

---

## Daily workflow (with Airflow)

1. Leave Airbyte connection schedule **Manual** (Airflow owns scheduling)
2. Airflow runs daily at **11:00 Thailand (Asia/Bangkok)** or trigger manually
3. On failure: check task logs → fix → clear failed task → re-run

Stop Airflow (data preserved):

```bash
cd airflow-platform && docker compose stop
```
