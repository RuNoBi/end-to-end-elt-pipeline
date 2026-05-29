{{
    silver_table_config(
        unique_key='country_id',
        bronze_table='countries',
        bronze_key='countryid',
        bronze_source='src_api_countries',
    )
}}

with

source as (

    select *
    from {{ source('src_api_countries', 'countries') }}

),

renamed as (

    select
        trim(countryid) as country_id,
        upper(trim(countrycode)) as country_code,
        trim(country) as country_name,
        cast(nullif(trim(importsequencenumber), '') as bigint) as import_sequence_number,
        cast(nullif(trim(statecode), '') as smallint) as state_code,
        cast(nullif(trim(statuscode), '') as smallint) as status_code,
        cast(_airbyte_extracted_at as timestamptz) as bronze_loaded_at,
        current_timestamp as dbt_updated_at
    from source
    where countryid is not null

),

deduplicated as (

    select distinct on (country_id)
        country_id,
        country_code,
        country_name,
        import_sequence_number,
        state_code,
        status_code,
        bronze_loaded_at,
        dbt_updated_at
    from renamed
    order by country_id asc, bronze_loaded_at desc

),

{# Source may assign the same ISO code to multiple GUIDs (e.g. CF → CAR vs Central African Republic). #}
deduped_by_country_code as (

    select distinct on (country_code)
        country_id,
        country_code,
        country_name,
        import_sequence_number,
        state_code,
        status_code,
        bronze_loaded_at,
        dbt_updated_at
    from deduplicated
    order by
        country_code asc,
        length(country_name) desc,
        bronze_loaded_at desc

),

final as (

    select *
    from deduped_by_country_code

)

select *
from final
