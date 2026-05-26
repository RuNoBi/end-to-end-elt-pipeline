with

orders as (

    select *
    from {{ ref('fct_orders') }}

),

customers as (

    select *
    from {{ ref('dim_customer') }}

),

joined as (

    select
        date_trunc('month', o.order_date) as order_month,
        o.customer_id,
        c.customer_name,
        c.customer_cohort_month,
        o.order_status,
        o.order_value_segment,
        o.order_amount
    from orders as o
    inner join customers as c
        on o.customer_id = c.customer_id

),

aggregated as (

    select
        order_month,
        customer_id,
        customer_name,
        customer_cohort_month,
        order_status,
        order_value_segment,
        count(*) as order_count,
        sum(order_amount) as total_revenue,
        avg(order_amount) as avg_order_value,
        min(order_amount) as min_order_value,
        max(order_amount) as max_order_value,
        current_timestamp as dbt_updated_at
    from joined
    group by
        order_month,
        customer_id,
        customer_name,
        customer_cohort_month,
        order_status,
        order_value_segment

)

select *
from aggregated
