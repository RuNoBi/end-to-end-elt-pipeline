# Pipeline: `sap_chemicals`

SAP chemical sales mock → Bronze `src_sap_chemicals` → **`silver_sap` / `gold_sap`**.

## Lineage

```text
src_sap_chemicals.sap_*  →  silver_sap.stg_sap_*
                                ↓
                         silver_sap.int_sap_sales_lines_enriched
                                ↓
                         gold_sap.dim_sap_* / fct_sap_sales_lines
                                ↓
                         gold_sap.mart_sap_chemical_sales_performance
```

## Run / test

```bash
make run-sap
make test-sap
```

CKAN: **`gold_sap.*`** — `airflow-platform/config/ckan/sap_chemicals.yaml`.
