# CKAN — Gold datamart catalog (localhost)

Separate **open data catalog** for consuming `gold.*` tables from the analytics warehouse.  
Warehouse Postgres (`warehouse-postgres`) stays private; CKAN holds a **published copy** in its **Datastore**.

## Architecture

```text
gold_sales.mart_sales_performance  ──Airflow publish──►  CKAN Datastore  ──►  UI / API
gold_sales.dim_customer
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

Then restart Airflow (token is synced automatically):

```bash
cd ../airflow-platform
docker compose up -d airflow-scheduler airflow-webserver
```

**After every `docker compose build ckan`** (or CKAN DB reset), run `bootstrap-ckan.sh` again + restart Airflow — see [docs/CKAN_SETUP.md](docs/CKAN_SETUP.md) §2.

Open **http://localhost:5001** — organization **ube-group-thailand**.

## Airflow

DAG `elt_main_pipeline` task group **`publication`** → `publish_gold_to_ckan` runs after Gold tests pass.

See [docs/CKAN_SETUP.md](docs/CKAN_SETUP.md).
