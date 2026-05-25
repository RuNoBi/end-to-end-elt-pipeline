{#
  Standard incremental table config for Silver staging models.
  Materializing as tables decouples Silver from Bronze at the Postgres catalog level,
  so Airbyte can recreate raw tables without CASCADE.
#}
{% macro silver_incremental_config(
    unique_key,
    extracted_at_column='_airbyte_extracted_at',
    bronze_table=none,
    bronze_key='id'
) %}
    {% set post_hooks = [
        "create index if not exists " ~ this.identifier ~ "_pk_idx on {{ this }} (" ~ unique_key ~ ")",
        "create index if not exists " ~ this.identifier ~ "_extracted_at_idx on {{ this }} (" ~ extracted_at_column ~ ")"
    ] %}
    {% if bronze_table is not none %}
        {% set post_hooks = post_hooks + [
            "{{ prune_keys_not_in_bronze('" ~ bronze_table ~ "', '" ~ unique_key ~ "', '" ~ bronze_key ~ "') }}"
        ] %}
    {% endif %}
    {{
        config(
            materialized='incremental',
            unique_key=unique_key,
            incremental_strategy='delete+insert',
            on_schema_change='sync_all_columns',
            post_hook=post_hooks
        )
    }}
{% endmacro %}
