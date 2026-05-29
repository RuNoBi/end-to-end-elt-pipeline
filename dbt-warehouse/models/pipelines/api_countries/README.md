# api_countries

Dynamics-style country JSON via **Airbyte** → `src_api_countries` → `silver_api.stg_countries` → `gold_api.dim_country`.

- **Bronze:** full refresh + overwrite (`countries` stream).
- **Silver / Gold:** `table` materialization (rebuild each run; matches reference-data ingest).
- **Dedupe:** one row per `country_code` in Silver (source may duplicate ISO codes, e.g. `CF`).

Setup: [docs/API_COUNTRIES_PIPELINE.md](../../../docs/API_COUNTRIES_PIPELINE.md).
