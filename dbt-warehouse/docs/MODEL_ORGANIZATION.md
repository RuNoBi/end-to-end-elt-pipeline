# dbt model organization

## Principles

1. **One folder per pipeline** — matches Airflow `config/pipelines/<pipeline_id>.yaml` and tag `pipeline_<pipeline_id>`.
2. **Medallion inside each pipeline** — `staging` → `intermediate` → `marts`.
3. **Separate Postgres schemas per pipeline** — no cross-pipeline clutter in DBeaver; ACL-ready.
4. **Test failures in `dbt_audit`** — only when `DBT_TARGET` is `dev` or `ci` (disabled in `prod`).
5. **Exposures** — `models/exposures.yml` links Gold marts to CKAN / Airflow consumers.
6. **Selectors** — `selectors.yml` for repeatable `--selector retail_pipeline` runs.

## Schema map

| Pipeline | Silver schema | Gold schema | Bronze |
|----------|---------------|-------------|--------|
| `sales_local_postgres` | `silver_sales` | `gold_sales` | `src_local_postgres` |
| `sap_chemicals` | `silver_sap` | `gold_sap` | `src_sap_chemicals` |
| Test audit (dev/ci) | — | — | `dbt_audit` |

## Repository layout

```text
dbt-warehouse/
├── models/pipelines/<pipeline_id>/
├── models/exposures.yml
├── selectors.yml
├── snapshots/pipelines/<pipeline_id>/
├── tests/pipelines/<pipeline_id>/
├── macros/
└── dbt_project.yml
```

## Targets (`profiles.yml`)

| Target | Use case | `store_failures` |
|--------|----------|------------------|
| `dev` | Local `make run` | yes → `dbt_audit` |
| `prod` | Airflow scheduled ELT | no |
| `ci` | GitHub Actions | yes → `dbt_audit` |

Set in `.env`: `DBT_TARGET=dev` (local) or `DBT_TARGET=prod` (Airflow).

## Debugging

| Layer | Where to look |
|-------|----------------|
| Bronze | `src_*` + `staging/_sources.yml` |
| Silver | `silver_<domain>.stg_*` / `int_*` |
| Gold | `gold_<domain>.*` |
| Failed test (dev) | `dbt_audit.*` |
| Compiled SQL | `target/compiled/.../pipelines/<id>/` |

## Schema migration (one-time)

```bash
make run-full
# then in DBeaver: analyses/migration_per_pipeline_schemas.sql
```

## Adding a pipeline

1. `models/pipelines/<pipeline_id>/` + README
2. Vars + config block in `dbt_project.yml` (`schema_silver_*`, `schema_gold_*`)
3. `airflow-platform/config/pipelines/<id>.yaml` + CKAN yaml with matching `gold_*` schema
4. Entry in `macros/create_medallion_schemas.sql` vars list (via new `var()` entries)
