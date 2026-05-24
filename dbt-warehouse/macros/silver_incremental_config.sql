{#
  Standard incremental table config for Silver staging models.
  Materializing as tables decouples Silver from Bronze at the Postgres catalog level,
  so Airbyte can recreate raw tables without CASCADE.
#}
{% macro silver_incremental_config(unique_key, extracted_at_column='_airbyte_extracted_at') %}
    {{
        config(
            materialized='incremental',
            unique_key=unique_key,
            incremental_strategy='delete+insert',
            on_schema_change='sync_all_columns',
            post_hook=[
                "create index if not exists " ~ this.identifier ~ "_pk_idx on {{ this }} (" ~ unique_key ~ ")",
                "create index if not exists " ~ this.identifier ~ "_extracted_at_idx on {{ this }} (" ~ extracted_at_column ~ ")"
            ]
        )
    }}
{% endmacro %}
