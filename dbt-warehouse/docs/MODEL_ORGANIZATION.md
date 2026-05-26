# dbt model organization

## Principles

1. **One folder per pipeline** — matches Airflow `config/pipelines/<pipeline_id>.yaml` and dbt tag `pipeline_<pipeline_id>`.
2. **Medallion inside each pipeline** — `staging` → `intermediate` → `marts`; no shared top-level `models/staging/`.
3. **Postgres schemas stay stable** — all Silver in `silver`, all Gold in `gold`; folder structure is for humans and CI, not extra DB schemas per pipeline.
4. **Test failures isolated** — `dbt_audit` schema (`store_failures: true`), so `silver` lists only business tables/views.
5. **Global model names** — `ref('stg_orders')` works project-wide; use tags/selectors to scope runs.

## Repository layout

```text
dbt-warehouse/
├── models/pipelines/
│   ├── README.md
│   ├── sales_local_postgres/
│   └── sap_chemicals/
├── macros/                    # shared: dedupe, incremental, prune
├── snapshots/pipelines/       # per-pipeline SCD2 (if any)
├── tests/pipelines/           # singular / custom SQL tests
├── analyses/                  # ad-hoc SQL, not deployed
└── dbt_project.yml            # layer defaults per pipeline
```

## Debugging production issues

| Layer | Schema | What to open first |
|-------|--------|-------------------|
| Bronze | `src_*` | Airbyte connection + `_sources.yml` freshness |
| Silver table | `silver` | `pipelines/<id>/staging/stg_*.sql` |
| Silver view | `silver` | `pipelines/<id>/intermediate/int_*.sql` |
| Gold | `gold` | `pipelines/<id>/marts/**` |
| Failed test | `dbt_audit` | Airflow `dbt_test_*` log → failing test name |

Compiled SQL after `dbt compile` or a failed run:

```text
target/compiled/de_poc_warehouse/models/pipelines/<pipeline_id>/...
```

## Adding a new pipeline

1. Create `models/pipelines/<pipeline_id>/` with `staging/`, `intermediate/`, `marts/`.
2. Add a block under `models.de_poc_warehouse.pipelines` in `dbt_project.yml` (copy an existing pipeline).
3. Add `airflow-platform/config/pipelines/<pipeline_id>.yaml` with `dbt.silver_run_select: "tag:pipeline_<pipeline_id>"`.
4. Register DAG in `elt_pipelines.py` if using the factory pattern.
5. Document in `models/pipelines/<pipeline_id>/README.md`.
