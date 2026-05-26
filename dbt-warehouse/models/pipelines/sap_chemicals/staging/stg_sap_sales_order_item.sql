{{
    silver_incremental_config(
        unique_key='sales_order_item_key',
        bronze_source='src_sap_chemicals',
    )
}}

with

source as (

    select *
    from {{ source('src_sap_chemicals', 'sap_sales_order_item') }}
    {{ get_raw_incremental_predicate('_airbyte_extracted_at') }}

),

renamed as (

    select
        trim(sales_order_number) || '-' || trim(item_number) as sales_order_item_key,
        trim(sales_order_number) as sales_order_number,
        trim(item_number) as item_number,
        trim(material_number) as material_number,
        upper(trim(plant_code)) as plant_code,
        cast(order_quantity as numeric(13, 3)) as order_quantity_mt,
        cast(net_value as numeric(15, 2)) as net_value,
        upper(trim(currency_code)) as currency_code,
        cast(item_changed_at as timestamptz) as item_changed_at,
        cast(_airbyte_extracted_at as timestamptz) as _airbyte_extracted_at,
        current_timestamp as dbt_updated_at
    from source
    where sales_order_number is not null
      and item_number is not null
      and material_number is not null

),

deduplicated as (

    {{ dedupe_airbyte_change_data(
        relation='renamed',
        unique_key='sales_order_item_key',
        order_by_columns=['_airbyte_extracted_at', 'item_changed_at'],
    ) }}

)

select *
from deduplicated
