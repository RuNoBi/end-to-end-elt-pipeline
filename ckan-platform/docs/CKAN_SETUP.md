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

```bash
docker compose build ckan
docker compose up -d
./scripts/bootstrap-ckan.sh
# or after token exists: ./scripts/configure-ube-catalog.sh
```

Copy output into `airflow-platform/.env`:

```bash
CKAN_URL=http://ckan:5000
CKAN_API_TOKEN=<token>
AIRFLOW_VAR_CKAN_API_TOKEN=<token>
CKAN_ORGANIZATION=ube-group-thailand
AIRFLOW_VAR_CKAN_ORGANIZATION=ube-group-thailand
CKAN_PUBLISH_MAX_ROWS=100000
AIRFLOW_VAR_CKAN_PUBLISH_MAX_ROWS=100000
CKAN_UPSERT_BATCH_SIZE=2000
AIRFLOW_VAR_CKAN_UPSERT_BATCH_SIZE=2000
```

Rebuild Airflow if you added psycopg2:

```bash
cd ../airflow-platform
docker compose build
docker compose up -d airflow-scheduler
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
| `sales-performance-mart` | `gold.mart_sales_performance` | Primary BI mart |
| `customer-dimension` | `gold.dim_customer` | Row limit via `CKAN_PUBLISH_MAX_ROWS` |

To add tables, edit `airflow-platform/dags/common/ckan_publish.py` → `GOLD_PUBLICATIONS`.

---

## 5. Troubleshooting

| Issue | Fix |
|-------|-----|
| CKAN unhealthy on Apple Silicon | Images use `platform: linux/amd64` — allow Rosetta / first start slow |
| `CKAN_API_TOKEN not set` | Run `bootstrap-ckan.sh`, update airflow `.env`, rebuild scheduler |
| Publish timeout / OOM (exit -9) | Lower `CKAN_PUBLISH_MAX_ROWS`; publication streams in batches (default 100k mart / 50k dim) |
| Empty dataset | Gold tests failed upstream — fix dbt first |
| `package_show` Not Found | Expected on first run — DAG creates packages automatically (fixed in `ckan_publish.py`) |
| Datastore permission errors | Re-run `bootstrap-ckan.sh` datastore set-permissions step |

---

## 6. Stop (keep data)

```bash
docker compose stop
```
