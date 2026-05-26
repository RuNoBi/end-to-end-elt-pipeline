{{
    silver_incremental_config(
        unique_key='material_number',
        bronze_table='sap_material',
        bronze_key='material_number',
        bronze_source='src_sap_chemicals',
    )
}}

with

source as (

    select *
    from {{ source('src_sap_chemicals', 'sap_material') }}
    {{ get_raw_incremental_predicate('_airbyte_extracted_at') }}

),

renamed as (

    select
        trim(material_number) as material_number,
        trim(material_description) as material_description,
        upper(trim(material_group)) as material_group,
        trim(product_hierarchy) as product_hierarchy,
        upper(trim(base_uom)) as base_uom,
        upper(trim(plant_code)) as plant_code,
        cast(updated_at as timestamptz) as updated_at,
        cast(_airbyte_extracted_at as timestamptz) as _airbyte_extracted_at,
        current_timestamp as dbt_updated_at
    from source
    where material_number is not null

),

deduplicated as (

    {{ dedupe_airbyte_change_data(
        relation='renamed',
        unique_key='material_number',
        order_by_columns=['_airbyte_extracted_at', 'updated_at'],
    ) }}

)

select *
from deduplicated
