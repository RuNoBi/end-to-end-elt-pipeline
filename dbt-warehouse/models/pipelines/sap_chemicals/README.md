# Pipeline: `sap_chemicals`

SAP chemical sales mock — `sap.*` on source Postgres → Airbyte `src_sap_chemicals` → dbt → CKAN.

## Lineage

```text
src_sap_chemicals.sap_material
src_sap_chemicals.sap_business_partner
src_sap_chemicals.sap_sales_order
src_sap_chemicals.sap_sales_order_item
        ↓
silver.stg_sap_*  →  silver.int_sap_sales_lines_enriched
        ↓
gold.dim_sap_customer, gold.dim_sap_material
gold.fct_sap_sales_lines  →  gold.mart_sap_chemical_sales_performance
```

## Run / test

```bash
make run-sap
make test-sap
```

Airflow: `elt_sap_chemicals` — `airflow-platform/config/pipelines/sap_chemicals.yaml`.

See [../../../../docs/SAP_CHEMICALS_PIPELINE.md](../../../../docs/SAP_CHEMICALS_PIPELINE.md) for source setup.
