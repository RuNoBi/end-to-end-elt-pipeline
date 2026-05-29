# Bronze sync watermarks

Production-style ingest SLA: **“Did Airbyte complete a sync recently?”**

| Piece | Role |
|-------|------|
| `bronze_meta.sync_watermarks` | One row per Bronze schema; updated by Airflow after each successful sync |
| `source:bronze_meta.watermark_<schema>` | dbt `source freshness` on `synced_at` (filtered by `bronze_schema`) |
| Raw `src_*` sources | Still use `_airbyte_extracted_at` for incremental models — **no** ingest SLA on row timestamps |

Incremental loads with unchanged source data do **not** advance `max(_airbyte_extracted_at)`; the watermark still advances when the job succeeds.

Add a new pipeline: extend `_sources.yml` with a `watermark_<bronze_schema>` entry and set `airbyte.bronze_schema` in pipeline YAML.
