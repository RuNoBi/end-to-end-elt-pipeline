# Documentation index

Use this folder as the “runbook + design docs” for the ELT platform.

## New here (recommended order)

1. `RUN_STEP_BY_STEP.md` — Thai step-by-step to bring the full stack up locally
2. `PLATFORM_LEARNING_GUIDE.md` — **Thai learning guide**: folder-by-folder overview + `sales_local_postgres` E2E workflow (file-by-file)
3. `CREDENTIALS.md` — which `.env` controls what (copy/paste friendly)
4. `MULTI_PIPELINE_ARCHITECTURE.md` — how one YAML becomes one Airflow DAG
5. `PRODUCTION_CHECKLIST.md` — preflight checks before daily runs

## First-day quick path

1. Prepare `.env` files (see `CREDENTIALS.md`)
2. Bring up services with `RUN_STEP_BY_STEP.md`
3. Validate first DAG run in Airflow
4. Verify CKAN catalog and Data Explorer

## Learning & architecture

- `PLATFORM_LEARNING_GUIDE.md` — platform folders, YAML wiring, retail pipeline workflow (reference for onboarding)

## Pipelines

- `MULTI_PIPELINE_ARCHITECTURE.md` — one YAML → one DAG; how to add pipelines
- `SAP_CHEMICALS_PIPELINE.md` — SAP pipeline setup (one source DB, two Airbyte connections)
- `API_COUNTRIES_PIPELINE.md` — countries API via Airbyte (HTTP → Bronze → dbt → CKAN)

## Operating the platform

- `PRODUCTION_CHECKLIST.md` — production readiness checklist (configs, row caps, schedules)
- `BEST_PRACTICES_LOCAL.md` — local best practices (delete semantics / prune, CI, etc.)
- `ENV_AND_GIT_SECURITY.md` — what is safe to commit vs never commit

## Monitoring & drills

- `MONITORING_FAILURE_DRILL.md` — inject bad data, expect a failure, practice incident workflow
- `FAILURE_EMAIL_DEBUG_CHECKLIST.md` — how to debug a failure email (maps to Airflow tasks)

## CKAN catalog

See `ckan-platform/docs/CKAN_SETUP.md` for CKAN bootstrap, token sync, and download semantics.

