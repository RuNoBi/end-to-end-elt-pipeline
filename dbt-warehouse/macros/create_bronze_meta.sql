{% macro create_bronze_meta() %}
    {% do run_query("create schema if not exists bronze_meta") %}
    {% do run_query(
        """
        create table if not exists bronze_meta.sync_watermarks (
            bronze_schema text primary key,
            connection_id text not null,
            pipeline_id text,
            synced_at timestamptz not null,
            job_id bigint,
            records_emitted bigint,
            records_committed bigint
        )
        """
    ) %}
{% endmacro %}
