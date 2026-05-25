# Localhost best practices — what we implemented

This document explains upgrades applied to the PoC so it behaves like a **senior DE local stack**, without production secrets (still `.env` on your machine).

---

## 1. Airflow pipeline (`elt_main_pipeline`)

### Before
```text
Airbyte → dbt run (all) → dbt test (all) → monitor
```

### After
```text
Airbyte
  → dbt source freshness     ← Bronze SLA (stale data fails fast)
  → dbt run Silver
  → dbt snapshot (SCD2)      ← customer history
  → dbt test Silver + sources + singular tests
  → dbt run Gold
  → dbt test Gold
  → monitor (always runs)
```

| Change | Why |
|--------|-----|
| **`dbt source freshness`** | `_sources.yml` already had 24h warn / 72h error — now enforced in the DAG |
| **Silver before Gold tests** | Bad data blocked before marts (already had; kept) |
| **`dbt deps` fails hard** | Removed `\|\| true` so broken packages cannot hide |
| **Email on failure** | Mailpit + `on_failure_callback` (unchanged) |

---

## 2. dbt — delete semantics (stale row fix)

| Mechanism | File |
|-----------|------|
| **Prune Silver keys** not in Bronze | `macros/prune_keys_not_in_bronze.sql` + post_hook on `stg_orders` / `stg_customers` |
| **Prune Gold facts** not in Silver | post_hook on `gold.fct_orders` |

After you **revert bad drill data** in source, the next `dbt run Silver` removes orphan keys automatically (no manual DELETE in warehouse for normal cases).

---

## 3. dbt — snapshots (SCD2)

| Artifact | Purpose |
|----------|---------|
| `snapshots/snap_stg_customers.sql` | Type-2 history for customer attributes in `gold` |
| Airflow task `dbt_snapshot_customers` | Runs snapshot after Silver build |

---

## 4. dbt — extra tests

| Test | Purpose |
|------|---------|
| `tests/singular/assert_no_orphan_orders_in_silver.sql` | Business rule: every Silver order has a Silver customer |
| Existing schema tests | Still on sources, staging, marts |

---

## 5. Source database reproducibility

| File | Purpose |
|------|---------|
| `source-postgres/init-source.sql` | DDL for **`customers` + `orders`** + sample ~1k orders |
| `source-postgres/scripts/seed-orders-1m.sql` | Optional **1M** rows for performance demos |

Fresh `docker compose up` on source now matches what dbt/Airbyte expect.

---

## 6. CI (GitHub Actions)

`.github/workflows/quality-gates.yml`:

- `dbt deps` + `parse` + `compile` (profile `ci` — no real DB)
- Python syntax check for Airflow DAGs

No production credentials in the workflow.

---

## 7. Airbyte config-as-code (starter)

| Path | Purpose |
|------|---------|
| `airbyte-platform/config/connection.template.json` | Documented connection shape |
| `airbyte-platform/scripts/export-connection.sh` | Pull live config from API → JSON |

UUID stays in `airflow-platform/.env`; JSON is for review and git.

---

## 8. Profiles

`dbt-warehouse/profiles.yml`:

- **`dev`** — Docker warehouse (default)
- **`ci`** — dummy host for `dbt parse` in GitHub Actions
- **`DBT_TARGET`** env override for tooling

---

## What we deliberately did NOT add (production-only)

- Real SMTP / Vault / cloud DW
- Celery/Kubernetes executor
- PagerDuty / Slack (email/Mailpit is enough on localhost)
- Elementary / OpenLineage (good next step)

---

## Quick commands

```bash
# dbt manual (same order as DAG)
cd dbt-warehouse && make freshness && make run && make snapshot && make test

# Export Airbyte connection
cd airbyte-platform && ./scripts/export-connection.sh

# CI locally
cd dbt-warehouse && make parse
```

See also: [MONITORING_FAILURE_DRILL.md](./MONITORING_FAILURE_DRILL.md), [AIRFLOW_ALERTING.md](../airflow-platform/docs/AIRFLOW_ALERTING.md).
