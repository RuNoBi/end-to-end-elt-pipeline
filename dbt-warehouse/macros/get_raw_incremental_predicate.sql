{#
  Filter Bronze (Airbyte) reads on incremental dbt runs.
  Uses _airbyte_extracted_at with a lookback window for late-arriving CDC rows.
#}
{% macro get_raw_incremental_predicate(extracted_at_column='_airbyte_extracted_at') %}
    {% if is_incremental() %}
        where {{ extracted_at_column }} >= (
            select coalesce(
                max({{ extracted_at_column }}) - interval '{{ var("incremental_lookback_days") }} days',
                '1970-01-01'::timestamptz
            )
            from {{ this }}
        )
    {% endif %}
{% endmacro %}
