# SAP chemicals mock pipeline (`sap_chemicals`)

Second ELT pipeline sharing **the same operational database** as retail sales — like production where **one MSSQL Server** holds multiple table groups (here: `public.*` + `sap.*` on `de_poc_source_postgres`).

## One source, two Bronze schemas

| Domain | Source (Postgres) | Airbyte connection | Bronze | Airflow DAG |
|--------|-------------------|--------------------|--------|-------------|
| Retail sales | `public.customers`, `public.orders` | connection #1 | `src_local_postgres` | `elt_main_pipeline` |
| SAP chemicals | `sap.sap_*` (4 tables) | connection #2 (same Source) | `src_sap_chemicals` | `elt_sap_chemicals` |

In Airbyte UI:

1. **One Source** → Postgres `de_poc_source_postgres:5432`, database `sales_source`
2. **Connection A** → namespace `src_local_postgres`, streams `customers`, `orders`
3. **Connection B** → namespace `src_sap_chemicals`, streams `sap_material`, `sap_business_partner`, `sap_sales_order`, `sap_sales_order_item` (schema **sap**)

### Airbyte ไม่เห็น stream ชื่อ sap (มีแค่ customers / orders)

ตาราง SAP อยู่ **schema `sap`** ไม่ใช่ `public` — ต้องให้ Source สแกน schema นี้ก่อน:

1. **Sources** → คลิก source Postgres ของคุณ → **Edit**
2. หา **Schemas** (หรือ *Allowed schemas* / *Schema*):
   - เปลี่ยนจากเฉพาะ `public` เป็น **`public` + `sap`**, หรือเลือก **All schemas**
3. **Save** → **Test connection**
4. กลับไปหน้า Connection (ขั้น Select streams) → กด **Refresh source schema**
5. ค้นหาในช่อง search: `sap` — ควรเห็น 4 streams (`sap_material`, …)

ถ้ายังไม่ขึ้น รันบนเครื่อง:

```bash
./source-postgres/scripts/apply-sap-schema.sh
```

แล้ว Refresh source schema อีกครั้ง

Template: `airbyte-platform/config/connection.sap_chemicals.template.json`

## Source tables (`schema sap`)

| Table | Role |
|-------|------|
| `sap.sap_material` | Chemical product master (UBE-style portfolio) |
| `sap.sap_business_partner` | Sold-to customers (SCG, PTT GC, …) |
| `sap.sap_sales_order` | Order header |
| `sap.sap_sales_order_item` | Lines (qty MT, net value) |

Fresh install: created automatically via `init-sap-chemicals.sql` on first `source-postgres` start.

**Existing volume** (DB already running):

```bash
chmod +x source-postgres/scripts/apply-sap-schema.sh
./source-postgres/scripts/apply-sap-schema.sh
```

Verify: `psql -h localhost -p 5433 -d sales_source -c '\dt sap.*'`

## Bronze without Airbyte (dev)

```bash
./scripts/bootstrap-sap-bronze.sh
```

## dbt & Airflow

```bash
cd dbt-warehouse
docker compose --profile tools run --rm dbt run --select tag:pipeline_sap_chemicals
```

```bash
# airflow-platform/.env
AIRFLOW_VAR_AIRBYTE_CONNECTION_ID_SAP_CHEMICALS=<connection-B-uuid>
```

DAG **`elt_sap_chemicals`** — schedule 11:30 Bangkok.

See [MULTI_PIPELINE_ARCHITECTURE.md](./MULTI_PIPELINE_ARCHITECTURE.md).
