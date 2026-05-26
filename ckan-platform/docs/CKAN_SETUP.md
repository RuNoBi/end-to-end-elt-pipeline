# CKAN setup — Gold datamart publication

## Best-practice pattern (localhost → production)

| Principle | This PoC |
|-----------|------------|
| **Separate catalog DB** | `ckan-db` — never mix with `data_warehouse` |
| **Publish, not direct query** | Airflow copies Gold → CKAN Datastore after tests pass |
| **Idempotent refresh** | `datastore_create(force=true)` + batched `datastore_upsert` each run |
| **Aggregated marts first** | Large facts (`fct_orders`) published with row caps; full history stays in the warehouse |
| **Gold tables in catalog** | `mart_sales_performance`, `dim_customer`, `dim_date`, `fct_orders` (see `ckan_publish.py` `GOLD_PUBLICATIONS`) |
| **API token auth** | Sysadmin token in `.env` (not in git) |

Production extensions: DCAT metadata, approval workflow, private orgs, object storage for large files, CKAN harvest from object store.

---

## 1. Start CKAN stack

```bash
cd ckan-platform
cp .env.example .env
docker compose build
docker compose up -d
docker compose ps   # all healthy
```

UI: http://localhost:5001  
Login: `CKAN_SYSADMIN_NAME` / `CKAN_SYSADMIN_PASSWORD` from `.env`

Branding: **UBE Group Thailand** theme (`ckanext-ube_theme`) — blue/white portal layout (data.go.th style): hero search, stats, featured datasets, custom header/footer (no “Powered by CKAN”). After theme changes: `docker compose build ckan && docker compose up -d ckan`.

---

## 2. Bootstrap API token + UBE catalog

### First time (or after CKAN DB reset / `docker compose build ckan`)

Run this **every time** you rebuild or recreate the CKAN database — the old API token in `.env` will not work (Airflow `publish_gold_to_ckan` fails with *Authorization Error*).

```bash
cd ckan-platform
./scripts/bootstrap-ckan.sh

cd ../airflow-platform
docker compose up -d airflow-scheduler airflow-webserver
```

What `bootstrap-ckan.sh` does:

- Datastore permissions
- Creates a new sysadmin API token (if the old one cannot edit the org)
- Writes `CKAN_API_TOKEN` to `ckan-platform/.env`
- Syncs token to `airflow-platform/.env` via `scripts/patch-ckan-env.sh`
- Runs `configure-ube-catalog.sh` (org, groups, domain tags, Data Explorer views)

You do **not** need to copy the token by hand unless `patch-ckan-env.sh` was skipped.

### CKAN already running, token still valid

```bash
cd ckan-platform
./scripts/configure-ube-catalog.sh   # groups / branding only, no new token
```

### Manual env reference (if needed)

```bash
CKAN_URL=http://ckan:5000
CKAN_API_TOKEN=<from bootstrap output>
AIRFLOW_VAR_CKAN_API_TOKEN=<same token>
CKAN_ORGANIZATION=ube-group-thailand
```

Rebuild Airflow image only when `requirements.txt` changed (e.g. added `psycopg2`):

```bash
cd ../airflow-platform
docker compose build airflow-scheduler
docker compose up -d airflow-scheduler airflow-webserver
```

---

## 3. Run end-to-end pipeline

Trigger **`elt_main_pipeline`** in Airflow. After success:

1. http://localhost:5001 → homepage hero → **Browse UBE datasets**
2. Organization **UBE Group Thailand** — Sales Performance Mart, Customer Dimension
3. Open a dataset → resource → tab **Data Explorer** (datatables preview)

---

## 4. Published datasets

| CKAN dataset | Warehouse table | Notes |
|--------------|-----------------|-------|
| `sales-performance-mart` | `gold_sales.mart_sales_performance` | Primary BI mart |
| `customer-dimension` | `gold_sales.dim_customer` | Row limit via `CKAN_PUBLISH_MAX_ROWS` |

To add tables, edit `airflow-platform/dags/common/ckan_publish.py` → `GOLD_PUBLICATIONS`.

---

## 5. Troubleshooting

| Issue | Fix |
|-------|-----|
| CKAN unhealthy on Apple Silicon | Images use `platform: linux/amd64` — allow Rosetta / first start slow |
| `CKAN_API_TOKEN not set` / **Authorization Error** on publish | Run §2 block above (`bootstrap-ckan.sh` + restart Airflow scheduler) |
| Publish timeout / OOM (exit -9) | Lower `CKAN_PUBLISH_MAX_ROWS`; publication streams in batches (default 100k mart / 50k dim) |
| Empty dataset | Gold tests failed upstream — fix dbt first |
| `package_show` Not Found | Expected on first run — DAG creates packages automatically (fixed in `ckan_publish.py`) |
| Datastore permission errors | Re-run `bootstrap-ckan.sh` datastore set-permissions step |

---

## 6. Stop (keep data)

```bash
docker compose stop
```
