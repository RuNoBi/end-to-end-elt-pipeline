{{ silver_incremental_config(unique_key='order_id', bronze_table='orders', bronze_key='id') }}

with

source as (

    select *
    from {{ source('src_local_postgres', 'orders') }}
    {{ get_raw_incremental_predicate('_airbyte_extracted_at') }}

),

renamed as (

    select
        cast(id as bigint) as order_id,
        cast(customer_id as bigint) as customer_id,
        cast(order_date as timestamptz) as order_date,
        cast(amount as numeric(18, 2)) as order_amount,
        lower(trim(status)) as order_status,
        cast(_airbyte_extracted_at as timestamptz) as _airbyte_extracted_at,
        current_timestamp as dbt_updated_at
    from source
    where id is not null
      and customer_id is not null

),

deduplicated as (

    {{ dedupe_airbyte_change_data(
        relation='renamed',
        unique_key='order_id',
        order_by_columns=['_airbyte_extracted_at', 'order_date']
    ) }}

),

final as (

    select
        order_id,
        customer_id,
        order_date,
        order_amount,
        order_status,
        _airbyte_extracted_at,
        dbt_updated_at
    from deduplicated

)

select *
from final
