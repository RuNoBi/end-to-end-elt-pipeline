# Failure email debug checklist

Use this when you receive **`[ELT ALERT][FAILED]`** from Airflow.  
Read the email **top → bottom**; each section maps to one debug step.

Related: [AIRFLOW_ALERTING.md](../airflow-platform/docs/AIRFLOW_ALERTING.md) · [MONITORING_FAILURE_DRILL.md](./MONITORING_FAILURE_DRILL.md)

---

## Quick triage (2 minutes)

- [ ] Note **DAG** (e.g. `elt_sap_chemicals`, `elt_main_pipeline`)
- [ ] Note **Task** (which layer failed)
- [ ] Open **Airflow Grid** for the **Run ID** in the email → confirm red task
- [ ] Decide if this is a **drill** (injected bad data) or a **real** incident

| Task contains | Layer | Usually means |
|---------------|-------|----------------|
| `extraction` | Bronze ingest | Airbyte sync failed |
| `dbt_source_freshness` | Bronze SLA | Data too stale / DB connection |
| `dbt_run_silver` | Silver build | SQL/model error |
| `dbt_test_silver` | Silver gate | **Data quality test failed** |
| `dbt_run_gold` / `dbt_test_gold` | Gold | Mart/test issue |
| `publish_gold_to_ckan` | Catalog | CKAN API / token |

---

## Section 1 — Red headline (root cause)

**Email shows:** test name, `Got N result...`, table, sample key (e.g. `from_field=BP_NOT_EXIST`)

| Check | Action |
|-------|--------|
| [ ] `Got 1 result` (or small N) | Focus on **specific bad rows**, not full reload |
| [ ] Table name present | Query that schema.table first |
| [ ] Sample key present | Use in `WHERE` when inspecting warehouse |

**Write down:** failing test · table · bad key value

---

## Section 2 — DAG / Task / Run

| Check | Action |
|-------|--------|
| [ ] Task is `dbt_test_silver` | Silver gate blocked Gold — fix data/tests before Gold |
| [ ] Upstream tasks green | Problem is local to this task |
| [ ] Upstream red | Fix **first red task left-to-right**, not this one yet |

---

## Section 3 — Facts table (Test / Config / dbt / Table)

| Field | What to do |
|-------|------------|
| **Test** | Copy name for `dbt test --select <name>` |
| **Config** | Open YAML path — see which column/test failed |
| **dbt** `Got N result...` | Confirms failure count from dbt |
| **Run** `ERROR=1` | One test failed in that run |
| **Table** | Primary place to inspect/fix data |

**SAP example config:** `dbt-warehouse/models/pipelines/sap_chemicals/staging/_sources.yml`

---

## Section 4 — Violating keys (live warehouse preview)

If the email shows a **table of violating keys**, those rows were queried from the warehouse when the alert fired.

| Check | Action |
|-------|--------|
| [ ] Re-run SELECT in warehouse (DBeaver / psql) | Confirm rows still exist |
| [ ] Match email sample | Same keys → proceed to fix/delete |
| [ ] Zero rows now | Data changed since test — re-run `dbt test` to confirm |

**Example (orphan partner on SAP Bronze):**

```sql
SELECT sales_order_number, partner_number
FROM src_sap_chemicals.sap_sales_order
WHERE partner_number NOT IN (
  SELECT partner_number FROM src_sap_chemicals.sap_business_partner
);
```

---

## Section 5 — Next steps in email

| Step in email | Do this |
|---------------|---------|
| `Re-run: dbt test --select ...` | After fix — verify test passes |
| `DELETE FROM ...` | Only if rows are invalid/drill — **review before running** |

### Run dbt test locally (Mac)

dbt does **not** load `.env` automatically.

```bash
cd dbt-warehouse
set -a && source .env && set +a
export DBT_WAREHOUSE_HOST=localhost
export DBT_WAREHOUSE_PORT=5434
export DBT_TARGET=dev

dbt test --select <test_name_from_email>
```

**Or use Docker (recommended):**

```bash
cd dbt-warehouse
make test-sap          # SAP pipeline
make test-sales        # Retail pipeline
```

### Run dbt test inside Airflow (same env as DAG)

```bash
cd airflow-platform
docker compose exec -it airflow-scheduler bash -lc '
  cd /opt/dbt && export DBT_PROFILES_DIR=/opt/dbt &&
  /home/airflow/.dbt-venv/bin/dbt test --profiles-dir /opt/dbt --select <test_name>
'
```

---

## Section 6 — Compiled test SQL

| Check | Action |
|-------|--------|
| [ ] Read child/parent tables in SQL | Know which tables are joined |
| [ ] Run full SQL in warehouse | Every returned row is a current violation |
| [ ] Compare with Violating keys table | Should match |

**Relationship tests:** `WHERE parent ... IS NULL` → orphan foreign keys in child table.

---

## Section 7 — dbt log (failure block)

| Check | Action |
|-------|--------|
| [ ] `Failure in test` line matches facts table | Same test name |
| [ ] `Done. PASS=... ERROR=1` | Only one failure |
| [ ] Link **Full logs in Airflow** if anything unclear | Full bash/dbt output |

---

## Fix paths by failure type

### A) Silver data test (`dbt_test_silver`) — e.g. relationships

1. [ ] Identify bad key from email / Violating keys / compiled SQL  
2. [ ] Fix or delete bad rows in **Bronze** (or fix source + re-sync)  
3. [ ] `dbt test --select <failing_test>` → PASS  
4. [ ] Re-trigger DAG (or clear failed task + downstream) → full run green  

**SAP drill cleanup example:**

```sql
DELETE FROM src_sap_chemicals.sap_sales_order
WHERE partner_number = 'BP_NOT_EXIST';
-- or: WHERE sales_order_number = 'DRILL-SAP-FAIL-001';
```

### B) Bronze freshness (`dbt_source_freshness`)

1. [ ] Check `extraction` / Airbyte sync succeeded  
2. [ ] Confirm `_airbyte_extracted_at` moved forward on Bronze tables  
3. [ ] `dbt source freshness --select source:<bronze_schema>`  

### C) Silver build (`dbt_run_silver`)

1. [ ] Open task log — first SQL/database error  
2. [ ] `dbt run --select <failing_model>`  
3. [ ] Check `DBT_WAREHOUSE_*` if connection error  

### D) Airbyte (`extraction`)

1. [ ] Airbyte UI → failed job → stream error  
2. [ ] Fix connector / schema / credentials  
3. [ ] Re-trigger DAG  

---

## Done criteria

- [ ] Failing `dbt test` (or task) passes in isolation  
- [ ] Re-triggered DAG run: failed task **green**  
- [ ] Downstream tasks ran (e.g. Gold after Silver gate)  
- [ ] Drill data removed if this was a test (no orphan rows left)  

---

## One-page flow

```text
Email headline → what failed + which key
     ↓
Violating keys / compiled SQL → prove in warehouse
     ↓
Fix Bronze or source
     ↓
dbt test --select <test>  (or make test-sap)
     ↓
Trigger DAG → confirm green
```

---

## References

| Topic | Doc |
|-------|-----|
| Inject SAP failure (drill) | SQL in chat / `docs/SAP_CHEMICALS_PIPELINE.md` |
| Retail drill | `docs/MONITORING_FAILURE_DRILL.md` |
| Alert setup | `airflow-platform/docs/AIRFLOW_ALERTING.md` |
| dbt commands in Docker | `dbt-warehouse/Makefile` |
