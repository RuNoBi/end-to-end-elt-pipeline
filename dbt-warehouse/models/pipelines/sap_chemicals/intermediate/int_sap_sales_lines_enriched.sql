with

items as (

    select *
    from {{ ref('stg_sap_sales_order_item') }}

),

orders as (

    select *
    from {{ ref('stg_sap_sales_order') }}

),

materials as (

    select *
    from {{ ref('stg_sap_material') }}

),

partners as (

    select *
    from {{ ref('stg_sap_business_partner') }}

),

joined as (

    select
        i.sales_order_item_key,
        i.sales_order_number,
        i.item_number,
        i.material_number,
        m.material_description,
        m.material_group,
        m.product_hierarchy,
        i.plant_code,
        o.partner_number,
        p.partner_name,
        p.country_code,
        p.customer_group,
        p.industry_sector,
        o.order_date,
        cast(o.order_date as date) as order_date_key,
        o.sales_organization,
        o.currency_code as header_currency_code,
        o.incoterms,
        o.order_status,
        i.order_quantity_mt,
        i.net_value,
        i.currency_code as line_currency_code,
        case
            when i.order_quantity_mt > 0 then round(i.net_value / i.order_quantity_mt, 2)
            else null
        end as unit_price_per_mt,
        case
            when i.net_value >= 500000 then 'large'
            when i.net_value >= 100000 then 'medium'
            else 'standard'
        end as deal_size_segment,
        i._airbyte_extracted_at
    from items as i
    inner join orders as o
        on i.sales_order_number = o.sales_order_number
    inner join materials as m
        on i.material_number = m.material_number
    inner join partners as p
        on o.partner_number = p.partner_number

)

select *
from joined
