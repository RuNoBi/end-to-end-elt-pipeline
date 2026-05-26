{{
    config(
        materialized='incremental',
        unique_key='sales_order_item_key',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        post_hook=[
            "create index if not exists {{ this.identifier }}_order_date_key_idx on {{ this }} (order_date_key)",
            "create index if not exists {{ this.identifier }}_material_number_idx on {{ this }} (material_number)",
            "create index if not exists {{ this.identifier }}_partner_number_idx on {{ this }} (partner_number)",
        ],
    )
}}

with

lines as (

    select *
    from {{ ref('int_sap_sales_lines_enriched') }}

    {% if is_incremental() %}
        where order_date >= (
            select coalesce(
                max(order_date) - interval '{{ var("incremental_lookback_days") }} days',
                '1970-01-01'::timestamptz
            )
            from {{ this }}
        )
    {% endif %}

)

select
    sales_order_item_key,
    sales_order_number,
    item_number,
    material_number,
    material_description,
    material_group,
    product_hierarchy,
    plant_code,
    partner_number,
    partner_name,
    country_code,
    customer_group,
    industry_sector,
    order_date,
    order_date_key,
    sales_organization,
    header_currency_code,
    line_currency_code,
    incoterms,
    order_status,
    order_quantity_mt,
    net_value,
    unit_price_per_mt,
    deal_size_segment,
    _airbyte_extracted_at as last_extracted_at,
    current_timestamp as dbt_updated_at
from lines
