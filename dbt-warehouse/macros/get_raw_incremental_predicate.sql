{#
  Filter Bronze (Airbyte) reads on incremental dbt runs.
  Bronze column (extracted_at_column) may differ from Silver {{ this }} (silver_extracted_at_column).
#}
{% macro get_raw_incremental_predicate(
    extracted_at_column='_airbyte_extracted_at',
    silver_extracted_at_column=none
) %}
    {% set silver_col = silver_extracted_at_column or extracted_at_column %}
    {% if is_incremental() %}
        where {{ extracted_at_column }} >= (
            select coalesce(
                max({{ silver_col }}) - interval '{{ var("incremental_lookback_days") }} days',
                '1970-01-01'::timestamptz
            )
            from {{ this }}
        )
    {% endif %}
{% endmacro %}
