{% snapshot snap_stg_customers %}

{{
    config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='_airbyte_extracted_at',
        invalidate_hard_deletes=True,
        tags=['pipeline_sales_local_postgres'],
    )
}}

{# Do not select dbt_updated_at — it duplicates the snapshot updated_at handling #}
select
    customer_id,
    customer_name,
    email,
    created_at,
    _airbyte_extracted_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
