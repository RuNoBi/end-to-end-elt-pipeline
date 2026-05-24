{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        post_hook=[
            "create index if not exists {{ this.identifier }}_order_date_key_idx on {{ this }} (order_date_key)",
            "create index if not exists {{ this.identifier }}_customer_id_idx on {{ this }} (customer_id)",
            "create index if not exists {{ this.identifier }}_order_status_idx on {{ this }} (order_status)"
        ]
    )
}}

with

orders as (

    select
        order_id,
        customer_id,
        order_date,
        order_date_key,
        order_amount,
        order_status,
        order_value_segment,
        _airbyte_extracted_at
    from {{ ref('int_orders_enriched') }}

    {% if is_incremental() %}
        where order_date >= (
            select coalesce(
                max(order_date) - interval '{{ var("incremental_lookback_days") }} days',
                '1970-01-01'::timestamptz
            )
            from {{ this }}
        )
    {% endif %}

),

final as (

    select
        order_id,
        customer_id,
        order_date,
        order_date_key,
        order_amount,
        order_status,
        order_value_segment,
        _airbyte_extracted_at as last_extracted_at,
        current_timestamp as dbt_updated_at
    from orders

)

select *
from final
