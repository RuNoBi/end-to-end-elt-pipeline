# API countries pipeline (`elt_api_countries`) — Airbyte only

Reference pipeline: **HTTP API (Dynamics-style JSON) → Airbyte → Bronze → dbt → CKAN**.

Ingestion is **Airbyte only** (same pattern as retail / SAP). There is no Airflow HTTP task or fixture JSON in this repo.

---

## Architecture

```text
extraction.trigger_airbyte_sync   ← Airbyte HTTP/API connector
    ↓
validation.dbt_source_freshness   ← bronze_meta.watermark_src_api_countries
    ↓
transformation                    ← stg_countries → dim_country
    ↓
publication.publish_gold_to_ckan
```

| Layer | Object | Rows (typical) |
|-------|--------|----------------|
| Bronze | `src_api_countries.countries` | 247 (raw from API) |
| Silver | `silver_api.stg_countries` | 246 (dedupe duplicate ISO codes) |
| Gold | `gold_api.dim_country` | 246 |

Silver collapses duplicate `country_code` values (e.g. `CF` for both “CAR” and “Central African Republic”) — prefer longest `country_name`, then latest `bronze_loaded_at`.

---

## คู่มือย่อ — UBE CRM QAS (ทำตามลำดับ)

### A. เตรียม Airbyte (ครั้งเดียว)

```bash
# ใน airbyte-platform/.env (แก้ bug Builder Test)
CONNECTOR_BUILDER_SERVER_API_HOST=http://airbyte-connector-builder-server:8080
CONNECTOR_BUILDER_API_HOST=airbyte-connector-builder-server:8080

cd airbyte-platform
docker compose up -d
docker compose up -d server   # โหลด env ใหม่ให้ airbyte-server
```

ทด API จากเครื่อง:

```bash
curl -sS "https://ugtwebportal.ube.co.th/ugt-api-crm-qas/Master/country" | head -c 300
```

### B. Connector Builder

| ขั้น | ทำอะไร |
|------|--------|
| 1 | http://localhost:8000 → **Builder** → Start from scratch |
| 2 | ชื่อ: `UBE CRM QAS Countries` |
| 3 | **Global config:** Base URL = `https://ugtwebportal.ube.co.th/ugt-api-crm-qas`, Auth = **No Auth** |
| 4 | **Stream +:** name `countries`, GET, path `/Master/country`, ไม่ตั้ง record selector |
| 5 | **Test** → เห็น `countryid`, `countrycode`, `country` |
| 6 | **Publish** to workspace |

### C. Source + Connection

| ขั้น | ทำอะไร |
|------|--------|
| 7 | **Sources** → New → เลือก `UBE CRM QAS Countries` → Test → Save |
| 8 | **Connections** → New → Source ด้านบน + Destination Postgres warehouse |
| 9 | Namespace **Custom:** `src_api_countries` |
| 10 | Stream **`countries`**, Sync **Full refresh** + **Overwrite**, Schedule **Manual** |
| 11 | **Sync now** → ตรวจ `src_api_countries.countries` ใน warehouse |
| 12 | Copy **Connection UUID** |

### D. Airflow + DAG

```bash
# airflow-platform/.env
AIRFLOW_VAR_AIRBYTE_CONNECTION_ID_API_COUNTRIES=<uuid>

cd airflow-platform
# Recreate containers so new AIRFLOW_VAR_* from .env is loaded (restart alone is not enough)
docker compose up -d airflow-scheduler airflow-webserver
```

Airflow → เปิด **`elt_api_countries`** → **Trigger DAG**

### E. ตรวจผล

```sql
SELECT country_id, country_code, country_name FROM gold_api.dim_country LIMIT 10;
```

CKAN: domain **API Reference Data** → **Country Reference (API)**

---

## Step-by-step — Airbyte (Phase C)

### 0) Prerequisites

- Stack up: `warehouse-postgres`, `airbyte-platform`, `airflow-platform`, `dbt-warehouse` on `de_poc_network`
- Retail or SAP pipeline already working (proves Airbyte → warehouse path)

### 1) Create **Destination** (if not reused)

Use the **same Postgres warehouse destination** as other pipelines:

| Field | Value |
|-------|--------|
| Host | `de_poc_warehouse_postgres` (from Airbyte container network) |
| Database | `data_warehouse` |
| User / password | from `warehouse-postgres/.env` |

### 2) Create **Source** (HTTP / API)

> **ไม่มี connector ชื่อ “API” ใน catalog** — นี่เป็นเรื่องปกติ  
> HTTP/REST ส่วนใหญ่ใช้ **Connector Builder** (สร้าง custom source) ไม่ใช่เลือกจากรายการ Postgres/SAP

#### ทาง A — Connector Builder (แนะนำ)

1. เปิด Airbyte UI → http://localhost:8000  
2. เมนูซ้าย → **Builder** (ไม่ใช่ Sources → New source แล้วหา “API”)  
3. **Start from scratch** / **New custom connector**  
4. ตั้งชื่อ เช่น `Countries HTTP API`  
5. **API Base URL** = ส่วน root ของ API (เช่น `https://api.example.com/v1/` — ไม่รวม path ท้ายถ้าแยก stream)  
6. **Authentication** = Bearer / API Key / OAuth ตาม API จริง  
7. เพิ่ม **Stream** ชื่อ **`countries`** (ต้องตรงกับ pipeline YAML):
   - **HTTP Method:** GET  
   - **URL / path:** endpoint ที่คืนรายการประเทศ (เช่น `/countries` หรือ OData `/api/data/v9.2/...`)  
   - **Record selector:** ถ้า response เป็น `{ "value": [ {...}, ... ] }` → ชี้ไปที่ `value`  
   - ทดใน Builder → **Publish to workspace**  
8. กลับไป **Sources → New source** → ค้นชื่อ connector ที่ publish (เช่น `Countries HTTP API`) — **ไม่ใช่** ค้นคำว่า API ทั่วไป

PoC stack มี `airbyte-connector-builder-server` ใน `docker-compose` แล้ว — ถ้าไม่เห็นเมนู Builder ให้ `docker compose ps` ว่า container นั้นรันอยู่

#### ทาง B — File (JSON) ชั่วคราว

ใช้ได้ถ้า API คืน **ไฟล์ JSON ตายตัว** หรือคุณวาง JSON บน URL โดยตรง:

- Sources → ค้น **`File`**  
- Format: JSON, URL = `http(s)://.../countries.json`  

จำกัด: auth ซับซ้อน / pagination / OData มักใช้ Builder ดีกว่า

---

### 2b) ตัวอย่าง UBE CRM QAS — `Master/country`

Endpoint จริง (QAS):

```text
https://ugtwebportal.ube.co.th/ugt-api-crm-qas/Master/country
```

#### ก่อนเปิด Builder — ทด API นอก Airbyte

1. เปิด **Postman** หรือ browser (ถ้า API อนุญาต GET ไม่มี auth)
2. `GET` URL ด้านบน + header ที่ทีม API ให้ (มักเป็น Bearer หรือ API Key)
3. ดู **รูปแบบ response** แล้วจด:

| รูปแบบที่เห็น | Record selector ใน Builder |
|--------------|----------------------------|
| `[ { "countryid": ... }, ... ]` | ไม่ต้องตั้ง (array ตรงๆ) |
| `{ "data": [ ... ] }` | `data` |
| `{ "value": [ ... ] }` | `value` |
| `{ "result": [ ... ] }` | `result` |
| object เดียว `{ "countryid": ... }` | ใช้ stream เดียว / อาจต้อง wrap |

ถ้า Docker บน Mac **เรียก URL นี้ไม่ได้** (VPN / firewall / SSL ภายใน) — แก้ network ก่อน; Airbyte ใน container จะ fail เหมือนกัน

#### Connector Builder — ค่าที่แนะนำ

**Global configuration**

| ช่อง | ค่า |
|------|-----|
| Connector name | `UBE CRM QAS Countries` |
| API Base URL | `https://ugtwebportal.ube.co.th/ugt-api-crm-qas` |
| Authentication | ตามที่ทีม API กำหนด (ด้านล่าง) |

**Stream `countries`** (ชื่อ stream ต้องเป็น **`countries`** ให้ตรง pipeline)

| ช่อง | ค่า |
|------|-----|
| Name | `countries` |
| HTTP Method | `GET` |
| Path / URL | `/Master/country` |
| Record selector | ตามตารางด้านบน (จาก Postman) |

URL เต็มที่ Airbyte จะเรียก:

```text
https://ugtwebportal.ube.co.th/ugt-api-crm-qas/Master/country
```

**Authentication (เลือกอย่างใดอย่างหนึ่ง — ถามทีม API)**

| แบบที่ API ใช้ | ตั้งใน Builder |
|----------------|----------------|
| Bearer token | Authenticator → Bearer → Header `Authorization` → `Bearer {{ config['token'] }}` แล้วสร้าง User input ชื่อ `token` |
| API Key ใน header | API Key → Inject **Header** → ชื่อ header ตาม doc (เช่น `X-Api-Key`) |
| API Key ใน query | API Key → Inject **Query** → ชื่อ param ตาม doc |

กด **Testing values** → ใส่ token/key จริงชั่วคราว → **Test read** จนเห็น `countryid`, `countrycode`, `country` ใน preview

**Publish** → Publish to workspace

**Expected record shape** (field names lowercase in Bronze):

```json
{
  "countryid": "111a1aa1-1111-aa11-aa11-111a1a11a111",
  "countrycode": "AA",
  "country": "Afghanistan",
  "importsequencenumber": "112",
  "statecode": "0",
  "statuscode": "1"
}
```

### 3) Create **Connection**

| Setting | Value |
|---------|--------|
| Source | your API source |
| Destination | warehouse Postgres |
| Namespace | **Custom** → `src_api_countries` |
| Stream | `countries` |
| Sync mode | **Full refresh** + **Overwrite** (reference data; switch to incremental later if API supports cursor) |
| Schedule | **Manual** (Airflow triggers sync) |

Copy the **Connection UUID** from the UI.

Template reference: `airbyte-platform/config/connection.api_countries.template.json`

### 4) Wire Airflow

Edit `airflow-platform/.env`:

```bash
AIRFLOW_VAR_AIRBYTE_CONNECTION_ID_API_COUNTRIES=<paste-connection-uuid>
```

Recreate Airflow (loads `AIRFLOW_VAR_*` from `.env`):

```bash
cd airflow-platform
docker compose up -d airflow-scheduler airflow-webserver
```

### 5) First sync test (Airbyte UI)

Before Airflow: **Sync now** once in Airbyte UI.

Verify Bronze:

```sql
SELECT countryid, countrycode, country, _airbyte_extracted_at
FROM src_api_countries.countries
LIMIT 10;
```

### 6) Run the DAG

1. Airflow UI → enable **`elt_api_countries`**
2. **Trigger DAG**
3. All tasks should go green: extraction → freshness → dbt → CKAN

Check Gold:

```sql
SELECT country_id, country_code, country_name, is_active
FROM gold_api.dim_country
ORDER BY country_code;
```

### 7) CKAN (optional)

Domain **API Reference Data** (`api-reference` group) → dataset **Country Reference (API)** (`country-dimension`).

After first publish (or if the domain card is missing on the homepage):

```bash
cd ckan-platform
./scripts/configure-ube-catalog.sh   # creates api-reference group + tags country-dimension
docker compose restart ckan          # reload theme catalog.py
```

If `configure-ube-catalog.sh` returns 403, refresh the API token:

```bash
./scripts/bootstrap-ckan.sh
cd ../airflow-platform && docker compose up -d airflow-scheduler airflow-webserver
```

---

## Pipeline YAML (already in repo)

`airflow-platform/config/pipelines/api_countries.yaml`:

- `airbyte.connection_id_variable: airbyte_connection_id_api_countries`
- `bronze_schema: src_api_countries`
- `expected_streams.countries`: full_refresh + overwrite (must match Airbyte UI or preflight fails)
- dbt Silver/Gold: `table` materialization (`silver_table_config` + `dim_country`) — not incremental CDC

---

## Troubleshooting

### Source check fail: `Could not find image: airbyte/source-declarative-manifest:4.6.2`

Custom connector จาก Builder ต้องมี Docker image นี้บนเครื่อง — **ไม่เกี่ยวกับ URL CRM**

```bash
cd airbyte-platform
./scripts/pull-custom-connector-images.sh
# หรือ manual:
docker pull airbyte/source-declarative-manifest:4.6.2
docker compose restart worker docker-proxy
```

กลับ Airbyte → **Test the source** อีกครั้ง

ยัง fail → ใช้ **File source** (ขั้น 2 ทาง B ด้านล่าง) แทน Builder — ได้ Bronze เหมือนกัน

### Builder Test: `baseUrl is invalid` (config ถูกแล้ว, curl ได้)

Airbyte **0.63** — `airbyte-server` ต้องรู้ที่อยู่ `connector-builder-server`:

1. แก้ `airbyte-platform/.env`:

```bash
CONNECTOR_BUILDER_SERVER_API_HOST=http://airbyte-connector-builder-server:8080
```

2. Recreate server (pull env ใหม่):

```bash
cd airbyte-platform
docker compose up -d server
```

3. กลับ Builder → Test stream **countries** อีกครั้ง

| Symptom | Fix |
|---------|-----|
| `Airflow Variable airbyte_connection_id_api_countries is not set` | Set `AIRFLOW_VAR_AIRBYTE_CONNECTION_ID_API_COUNTRIES` in `airflow-platform/.env`, ensure `docker-compose.yml` maps it, then `docker compose up -d airflow-scheduler airflow-webserver` |
| **`ERROR baseUrl is invalid`** (Builder Test, Base URL ถูกแล้ว) | Bug Airbyte **0.63**: set `CONNECTOR_BUILDER_SERVER_API_HOST` in `airbyte-platform/.env`, then `docker compose up -d server` |
| `unique` test fail on `country_code` | Source has duplicate ISO codes; Silver dedupes — run `dbt run --select stg_countries dim_country` then re-test |
| Bronze 247 rows, Silver 246 | Expected after dedupe (see Architecture table) |
| Freshness fail | Re-run extraction; check `bronze_meta.sync_watermarks` for `src_api_countries` |
| dbt: relation does not exist | Run Airbyte sync first — Bronze table must exist |
| Stream name not `countries` | Rename stream in Airbyte **or** update YAML + dbt `source` table name |

---

## Files

| Path | Purpose |
|------|---------|
| `airflow-platform/config/pipelines/api_countries.yaml` | DAG config |
| `airbyte-platform/config/connection.api_countries.template.json` | Connection shape |
| `dbt-warehouse/models/pipelines/api_countries/` | dbt models |
| `airflow-platform/config/ckan/api_countries.yaml` | CKAN publish |

---

## Local dbt only (after Bronze exists)

```bash
cd dbt-warehouse
make run-api
docker compose --profile tools run --rm dbt test --select tag:pipeline_api_countries
```
