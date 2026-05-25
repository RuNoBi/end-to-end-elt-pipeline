# Failure drill — bad source → Silver test fails

Use this guide to practice **monitoring a failed ELT run** the same way you would in production.

---

## What we simulate

| Scenario | Bad data in source | Expected failure |
|----------|-------------------|------------------|
| `orphan_order` (default) | Order with `customer_id = 999999999` | dbt test `relationships` on `stg_orders` |
| `duplicate_email` | Customer with duplicate `email` | dbt test `unique` on `stg_customers.email` |
| `both` | Both rows | First failing test in Silver gate |

**Best practice:** Airbyte (Bronze) still **succeeds** — bad data lands raw. **Silver tests** are the quality gate before Gold.

---

## 1 — Inject bad data (source DB)

```bash
cd source-postgres
chmod +x scripts/inject-bad-data.sh scripts/revert-bad-data.sh
./scripts/inject-bad-data.sh              # default: orphan_order
# ./scripts/inject-bad-data.sh duplicate_email
# ./scripts/inject-bad-data.sh both
```

---

## 2 — Run the pipeline

1. Open Airflow: http://localhost:8080  
2. DAG **`elt_main_pipeline`** → **Trigger DAG** (full run: Airbyte → dbt → monitor)

> Airbyte sync may take a long time if you have ~1M rows. For a **dbt-only** drill after Bronze already has the bad row, use **Grid** → select `transformation.dbt_run_silver` → **Run** (with “ignore upstream” if offered), or wait for one full sync.

---

## 3 — Monitor in Airflow (best practice)

### A. Grid view (daily driver)

| Color | Meaning | Action |
|-------|---------|--------|
| **Green** | Success | None |
| **Red** | Failed | Open task → **Log** |
| **Orange** | Upstream failed | Skipped because a parent failed — fix the **red** task first |
| **Pink** | Up for retry | Wait or check logs |

**Read left → right:** `extraction` → `transformation` → `monitoring`.

Expected drill pattern:

```text
extraction          [green]
transformation      dbt_run_silver [green]
                    dbt_test_silver [red]    ← start here
                    dbt_run_gold [orange]
                    dbt_test_gold [orange]
monitoring          log_pipeline_status [green]  ← always runs; check its log too
```

### B. Failed task — Logs (root cause)

1. Click the **red** square (`dbt_test_silver`).
2. Tab **Log** — search for:
   - `FAIL 1` / `Failure in test`
   - `relationships_stg_orders` or `unique_stg_customers_email`
3. dbt prints the **failing SQL** and row count.

### C. Graph view (dependencies)

**DAG → Graph** shows why Gold did not run: `dbt_test_silver` blocks `dbt_run_gold`.

### D. Monitoring task (run outcome summary)

Task **`monitoring.log_pipeline_status`** uses `trigger_rule=all_done` so it runs even when transformation fails.

Open its **Log** and look for:

```text
PIPELINE RUN SUMMARY | state=failed | failed_tasks=[...]
HINT | Silver gate failed — open Log on transformation.dbt_test_silver ...
```

### E. Gantt / Duration

Use **Gantt** to see whether Airbyte or dbt dominated runtime on past runs.

### F. Long-term habits

| Habit | Why |
|-------|-----|
| Pin **Grid** as default view | Fast scan of last N runs |
| Filter DAG runs by **Failed** | Triage without opening each run |
| Always read **failed task Log** first | Root cause lives there, not in monitoring |
| Use **monitoring** task for run-level summary | One place for failed task IDs |
| Keep **Silver test before Gold run** in DAG | Prevents bad data reaching marts |
| Document drills in runbooks | Same steps as this file |

Optional later: Slack/email `on_failure_callback`, OpenTelemetry, or Datadog — out of scope for this PoC.

---

## 4 — Revert and confirm green run

```bash
cd source-postgres
./scripts/revert-bad-data.sh
```

`revert-bad-data.sh` removes drill rows from **source**, **Bronze**, **Silver**, and **Gold**.

### Stale rows after revert (fixed in pipeline)

Silver/Gold models now run **prune post-hooks** after each incremental build — keys deleted in Bronze are removed from `stg_*` and `fct_orders` on the next `dbt run Silver`.

If tests still fail once: run **`./scripts/revert-bad-data.sh`** then trigger DAG (or `make run` in dbt-warehouse).

Trigger **`elt_main_pipeline`** again. All transformation tasks should be **green**.

---

## 5 — Verify in warehouse (optional)

```bash
docker exec de_poc_warehouse_postgres psql -U warehouse_admin -d data_warehouse -c \
  "SELECT id, customer_id, status FROM src_local_postgres.orders WHERE id >= 900000000 LIMIT 5;"
```

After revert + sync, drill rows should disappear from Bronze.

---

## Quick reference

| Step | Command / UI |
|------|----------------|
| Break | `./scripts/inject-bad-data.sh` |
| Run | Airflow → Trigger DAG |
| Diagnose | Grid → red `dbt_test_silver` → Log |
| Summary | `monitoring.log_pipeline_status` → Log |
| Fix data | `./scripts/revert-bad-data.sh` + re-trigger |
