with

orders as (

    select *
    from {{ ref('stg_orders') }}

),

enriched as (

    select
        order_id,
        customer_id,
        order_date,
        cast(order_date as date) as order_date_key,
        order_amount,
        order_status,
        case
            when order_amount >= 1000 then 'high'
            when order_amount >= 100 then 'medium'
            else 'low'
        end as order_value_segment,
        _airbyte_extracted_at
    from orders

)

select *
from enriched
