{{
    silver_incremental_config(
        unique_key='partner_number',
        bronze_table='sap_business_partner',
        bronze_key='partner_number',
        bronze_source='src_sap_chemicals',
    )
}}

with

source as (

    select *
    from {{ source('src_sap_chemicals', 'sap_business_partner') }}
    {{ get_raw_incremental_predicate('_airbyte_extracted_at') }}

),

renamed as (

    select
        trim(partner_number) as partner_number,
        trim(partner_name) as partner_name,
        upper(trim(country_code)) as country_code,
        trim(region) as region,
        trim(industry_sector) as industry_sector,
        upper(trim(customer_group)) as customer_group,
        cast(updated_at as timestamptz) as updated_at,
        cast(_airbyte_extracted_at as timestamptz) as _airbyte_extracted_at,
        current_timestamp as dbt_updated_at
    from source
    where partner_number is not null

),

deduplicated as (

    {{ dedupe_airbyte_change_data(
        relation='renamed',
        unique_key='partner_number',
        order_by_columns=['_airbyte_extracted_at', 'updated_at'],
    ) }}

)

select *
from deduplicated
