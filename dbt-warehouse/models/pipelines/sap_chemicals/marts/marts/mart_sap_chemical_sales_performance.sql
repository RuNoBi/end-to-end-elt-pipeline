with

lines as (

    select *
    from {{ ref('fct_sap_sales_lines') }}
    where order_status != 'CANCELLED'

),

aggregated as (

    select
        date_trunc('month', order_date)::date as order_month,
        material_group,
        product_hierarchy,
        plant_code,
        sales_organization,
        country_code,
        customer_group,
        order_status,
        deal_size_segment,
        count(*) as line_count,
        count(distinct sales_order_number) as order_count,
        count(distinct partner_number) as customer_count,
        sum(order_quantity_mt) as total_quantity_mt,
        sum(net_value) as total_net_value,
        avg(unit_price_per_mt) as avg_unit_price_per_mt,
        current_timestamp as dbt_updated_at
    from lines
    group by
        date_trunc('month', order_date)::date,
        material_group,
        product_hierarchy,
        plant_code,
        sales_organization,
        country_code,
        customer_group,
        order_status,
        deal_size_segment

)

select *
from aggregated
