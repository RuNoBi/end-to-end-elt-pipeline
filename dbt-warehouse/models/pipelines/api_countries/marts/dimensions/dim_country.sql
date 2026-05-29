{{
    config(
        materialized='table',
        post_hook=[
            "create index if not exists {{ this.identifier }}_pk_idx on {{ this }} (country_id)",
            "create index if not exists {{ this.identifier }}_code_idx on {{ this }} (country_code)",
        ],
    )
}}

with

countries as (

    select *
    from {{ ref('stg_countries') }}

),

final as (

    select
        country_id,
        country_code,
        country_name,
        import_sequence_number,
        state_code,
        status_code,
        (state_code = 0 and status_code = 1) as is_active,
        bronze_loaded_at as last_loaded_at,
        current_timestamp as dbt_updated_at
    from countries

)

select *
from final
