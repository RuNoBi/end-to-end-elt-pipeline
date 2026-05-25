{{ silver_incremental_config(unique_key='customer_id', bronze_table='customers', bronze_key='id') }}

with

source as (

    select *
    from {{ source('src_local_postgres', 'customers') }}
    {{ get_raw_incremental_predicate('_airbyte_extracted_at') }}

),

renamed as (

    select
        cast(id as bigint) as customer_id,
        trim(name) as customer_name,
        lower(trim(email)) as email,
        cast(created_at as timestamptz) as created_at,
        cast(_airbyte_extracted_at as timestamptz) as _airbyte_extracted_at,
        current_timestamp as dbt_updated_at
    from source
    where id is not null

),

deduplicated as (

    {{ dedupe_airbyte_change_data(
        relation='renamed',
        unique_key='customer_id',
        order_by_columns=['_airbyte_extracted_at', 'created_at']
    ) }}

),

final as (

    select
        customer_id,
        customer_name,
        email,
        created_at,
        _airbyte_extracted_at,
        dbt_updated_at
    from deduplicated

)

select *
from final
