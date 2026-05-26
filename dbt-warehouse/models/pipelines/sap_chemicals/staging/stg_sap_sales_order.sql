{{
    silver_incremental_config(
        unique_key='sales_order_number',
        bronze_table='sap_sales_order',
        bronze_key='sales_order_number',
        bronze_source='src_sap_chemicals',
    )
}}

with

source as (

    select *
    from {{ source('src_sap_chemicals', 'sap_sales_order') }}
    {{ get_raw_incremental_predicate('_airbyte_extracted_at') }}

),

renamed as (

    select
        trim(sales_order_number) as sales_order_number,
        trim(partner_number) as partner_number,
        cast(order_date as date) as order_date,
        cast(requested_delivery_date as date) as requested_delivery_date,
        upper(trim(sales_organization)) as sales_organization,
        trim(distribution_channel) as distribution_channel,
        trim(division) as division,
        upper(trim(currency_code)) as currency_code,
        upper(trim(incoterms)) as incoterms,
        upper(trim(order_status)) as order_status,
        cast(changed_at as timestamptz) as changed_at,
        cast(_airbyte_extracted_at as timestamptz) as _airbyte_extracted_at,
        current_timestamp as dbt_updated_at
    from source
    where sales_order_number is not null
      and partner_number is not null

),

deduplicated as (

    {{ dedupe_airbyte_change_data(
        relation='renamed',
        unique_key='sales_order_number',
        order_by_columns=['_airbyte_extracted_at', 'changed_at'],
    ) }}

)

select *
from deduplicated
