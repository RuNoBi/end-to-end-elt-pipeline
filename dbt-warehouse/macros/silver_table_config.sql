{#
  Full-refresh Silver table (reference / small Bronze snapshots).
  Pairs with Airbyte full_refresh + overwrite sources.
#}
{% macro silver_table_config(
    unique_key,
    bronze_table=none,
    bronze_key='id',
    bronze_source=var('raw_schema')
) %}
    {% set post_hooks = [
        "create index if not exists " ~ this.identifier ~ "_pk_idx on {{ this }} (" ~ unique_key ~ ")"
    ] %}
    {% if bronze_table is not none %}
        {% set post_hooks = post_hooks + [
            "{{ prune_keys_not_in_bronze('" ~ bronze_table ~ "', '" ~ unique_key ~ "', '" ~ bronze_key ~ "', '" ~ bronze_source ~ "') }}"
        ] %}
    {% endif %}
    {{
        config(
            materialized='table',
            on_schema_change='sync_all_columns',
            post_hook=post_hooks
        )
    }}
{% endmacro %}
