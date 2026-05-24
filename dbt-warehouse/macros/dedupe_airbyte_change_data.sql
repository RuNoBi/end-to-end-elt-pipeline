{#
  Deduplicate Airbyte CDC/raw tables using the latest extraction timestamp.
  Falls back to created_at / order_date when Airbyte metadata columns are absent.
#}
{% macro dedupe_airbyte_change_data(relation, unique_key, order_by_columns) %}
    select *
    from (
        select
            *,
            row_number() over (
                partition by {{ unique_key }}
                order by
                    {% for column in order_by_columns %}
                    {{ column }} desc nulls last{% if not loop.last %}, {% endif %}
                    {% endfor %}
            ) as _dbt_dedupe_rank
        from {{ relation }}
    ) as deduped
    where _dbt_dedupe_rank = 1
{% endmacro %}
