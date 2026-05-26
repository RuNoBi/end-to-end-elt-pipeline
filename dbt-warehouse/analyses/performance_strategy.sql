-- Post-run maintenance for per-pipeline Gold schemas (run manually or via cron).

analyze gold_sales.fct_orders;
analyze gold_sales.dim_customer;
analyze gold_sales.mart_sales_performance;

analyze gold_sap.fct_sap_sales_lines;
analyze gold_sap.dim_sap_material;
analyze gold_sap.mart_sap_chemical_sales_performance;
