{{
    config(
        materialized='incremental',
        unique_key='partner_number',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
    )
}}

select
    partner_number,
    partner_name,
    country_code,
    region,
    industry_sector,
    customer_group,
    date_trunc('month', updated_at)::date as partner_since_month,
    updated_at,
    _airbyte_extracted_at as last_extracted_at,
    current_timestamp as dbt_updated_at
from {{ ref('stg_sap_business_partner') }}

{% if is_incremental() %}
    where _airbyte_extracted_at >= (
        select coalesce(
            max(last_extracted_at) - interval '{{ var("incremental_lookback_days") }} days',
            '1970-01-01'::timestamptz
        )
        from {{ this }}
    )
{% endif %}
