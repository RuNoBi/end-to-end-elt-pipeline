# Documentation index

Use this folder as the “runbook + design docs” for the ELT platform.

## Start here

- `RUN_STEP_BY_STEP.md` — Thai step-by-step to bring the full stack up locally
- `CREDENTIALS.md` — which `.env` controls what (copy/paste friendly)
- `ENV_AND_GIT_SECURITY.md` — what is safe to commit vs never commit

## Operating the platform

- `PRODUCTION_CHECKLIST.md` — production readiness checklist (configs, row caps, schedules)
- `BEST_PRACTICES_LOCAL.md` — local best practices (delete semantics / prune, CI, etc.)

## Pipelines

- `MULTI_PIPELINE_ARCHITECTURE.md` — one YAML → one DAG; how to add pipelines
- `SAP_CHEMICALS_PIPELINE.md` — SAP pipeline setup (one source DB, two Airbyte connections)

## Monitoring & drills

- `MONITORING_FAILURE_DRILL.md` — inject bad data, expect a failure, practice incident workflow
- `FAILURE_EMAIL_DEBUG_CHECKLIST.md` — how to debug a failure email (maps to Airflow tasks)

## CKAN catalog

See `ckan-platform/docs/CKAN_SETUP.md` for CKAN bootstrap, token sync, and download semantics.

