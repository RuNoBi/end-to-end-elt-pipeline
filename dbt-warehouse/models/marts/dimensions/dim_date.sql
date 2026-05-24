{{
    config(
        materialized='table',
        post_hook=[
            "create unique index if not exists {{ this.identifier }}_date_key_idx on {{ this }} (date_key)"
        ]
    )
}}

with

bounds as (

    select
        min(order_date_key) as min_date,
        max(order_date_key) as max_date
    from {{ ref('int_orders_enriched') }}

),

date_spine as (

    select
        generate_series(
            (select min_date from bounds),
            (select max_date from bounds),
            interval '1 day'
        )::date as date_day
    from bounds
    where (select min_date from bounds) is not null

),

final as (

    select
        date_day as date_key,
        extract(year from date_day)::int as year_number,
        extract(quarter from date_day)::int as quarter_number,
        extract(month from date_day)::int as month_number,
        extract(week from date_day)::int as week_of_year,
        extract(dow from date_day)::int as day_of_week,
        trim(to_char(date_day, 'Day')) as day_name,
        trim(to_char(date_day, 'Month')) as month_name,
        (extract(dow from date_day) in (0, 6)) as is_weekend,
        current_timestamp as dbt_updated_at
    from date_spine

)

select *
from final
