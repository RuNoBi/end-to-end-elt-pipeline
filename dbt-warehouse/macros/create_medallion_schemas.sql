{% macro create_medallion_schemas() %}
    {# Per-pipeline medallion schemas + shared audit schema for dbt test failures #}
    {% set schemas = [
        var('schema_silver_sales'),
        var('schema_silver_sap'),
        var('schema_gold_sales'),
        var('schema_gold_sap'),
        'dbt_audit',
    ] %}
    {% for schema in schemas %}
        {% do run_query("create schema if not exists " ~ schema) %}
    {% endfor %}
{% endmacro %}
