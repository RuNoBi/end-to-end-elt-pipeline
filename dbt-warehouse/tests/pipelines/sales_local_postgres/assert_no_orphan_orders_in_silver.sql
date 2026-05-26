{{ config(tags=['pipeline_sales_local_postgres']) }}

-- Fails if Silver orders reference customers missing from Silver customers (business rule).
select
    o.order_id,
    o.customer_id
from {{ ref('stg_orders') }} as o
left join {{ ref('stg_customers') }} as c
    on c.customer_id = o.customer_id
where c.customer_id is null
