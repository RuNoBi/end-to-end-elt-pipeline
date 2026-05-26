# CKAN — Gold datamart catalog (localhost)

Separate **open data catalog** for consuming `gold.*` tables from the analytics warehouse.  
Warehouse Postgres (`warehouse-postgres`) stays private; CKAN holds a **published copy** in its **Datastore**.

## Architecture

```text
gold.mart_sales_performance  ──Airflow publish──►  CKAN Datastore  ──►  UI / API
gold.dim_customer
```

| Container | Role |
|-----------|------|
| `ckan` | Catalog UI + API (:5001) |
| `ckan-db` | CKAN metadata + Datastore DB (not `data_warehouse`) |
| `ckan-solr` | Search index |
| `ckan-redis` | Background jobs |
| `ckan-datapusher` | Optional CSV loads (enabled for standard stack) |

All attach to **`de_poc_network`** (same as Airflow / warehouse).

## Quick start

```bash
cp .env.example .env
docker compose build
docker compose up -d
# wait ~2–3 min for healthchecks
chmod +x scripts/bootstrap-ckan.sh
./scripts/bootstrap-ckan.sh
```

Paste the printed `AIRFLOW_VAR_CKAN_API_TOKEN` into `airflow-platform/.env`, then:

```bash
cd ../airflow-platform
docker compose build airflow-scheduler   # installs psycopg2 for publish task
docker compose up -d
```

Open **http://localhost:5001** — datasets under organization **de-poc-data**.

## Airflow

DAG `elt_main_pipeline` task group **`publication`** → `publish_gold_to_ckan` runs after Gold tests pass.

See [docs/CKAN_SETUP.md](docs/CKAN_SETUP.md).
