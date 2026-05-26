{#
  Remove Silver rows whose keys no longer exist in Bronze (source deletes / Airbyte reload).
  Runs as a model post_hook after incremental merge — localhost best practice for mirror semantics.
#}
{% macro prune_keys_not_in_bronze(
    bronze_table,
    silver_key,
    bronze_key='id',
    bronze_source=var('raw_schema')
) %}
delete from {{ this }} as s
where not exists (
    select 1
    from {{ source(bronze_source, bronze_table) }} as b
    where cast(b.{{ bronze_key }} as text) = cast(s.{{ silver_key }} as text)
)
{% endmacro %}
