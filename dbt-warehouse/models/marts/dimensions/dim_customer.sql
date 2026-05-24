{{
    config(
        unique_key='customer_id',
        post_hook=[
            "create index if not exists {{ this.identifier }}_pk_idx on {{ this }} (customer_id)"
        ]
    )
}}

with

customers as (

    select *
    from {{ ref('stg_customers') }}

    {% if is_incremental() %}
        where _airbyte_extracted_at >= (
            select coalesce(
                max(dbt_updated_at) - interval '{{ var("incremental_lookback_days") }} days',
                '1970-01-01'::timestamptz
            )
            from {{ this }}
        )
    {% endif %}

),

final as (

    select
        customer_id,
        customer_name,
        email,
        created_at as customer_created_at,
        date_trunc('month', created_at) as customer_cohort_month,
        current_timestamp as dbt_updated_at
    from customers

)

select *
from final
