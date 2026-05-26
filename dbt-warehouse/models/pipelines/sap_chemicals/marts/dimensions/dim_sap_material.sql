{{
    config(
        materialized='incremental',
        unique_key='material_number',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
    )
}}

select
    material_number,
    material_description,
    material_group,
    product_hierarchy,
    base_uom,
    plant_code,
    updated_at,
    _airbyte_extracted_at as last_extracted_at,
    current_timestamp as dbt_updated_at
from {{ ref('stg_sap_material') }}

{% if is_incremental() %}
    where _airbyte_extracted_at >= (
        select coalesce(
            max(last_extracted_at) - interval '{{ var("incremental_lookback_days") }} days',
            '1970-01-01'::timestamptz
        )
        from {{ this }}
    )
{% endif %}
