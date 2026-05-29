# คู่มือรันโปรเจกต์ทีละขั้น (end-to-end-elt-pipeline)

คู่มือนี้สมมติว่าคุณ clone repo แล้ว และมี Docker Desktop เปิดอยู่

---

## ก่อนเริ่ม — เตรียม `.env` ให้สอดคล้องกัน

รหัสผ่าน **warehouse** ต้องตรงกัน 3 ที่:

| ไฟล์ | ตัวแปร |
|------|--------|
| `warehouse-postgres/.env` | `POSTGRES_USER`, `POSTGRES_PASSWORD` |
| `dbt-warehouse/.env` | `DBT_WAREHOUSE_USER`, `DBT_WAREHOUSE_PASSWORD` |
| `airflow-platform/.env` | `DBT_WAREHOUSE_USER`, `DBT_WAREHOUSE_PASSWORD` |

ค่า PoC ที่ตั้งไว้แล้ว (ถ้ายังไม่เคยแก้):

```text
source:     source_admin / source_admin_pwd / sales_source
warehouse:  warehouse_admin / warehouse_admin_pwd / data_warehouse
airflow UI: admin / admin
```

ดูแผนภาพ credential เพิ่ม: [CREDENTIALS.md](CREDENTIALS.md)

```bash
cd /path/to/end-to-end-elt-pipeline

cp source-postgres/.env.example source-postgres/.env
cp warehouse-postgres/.env.example warehouse-postgres/.env
cp airbyte-platform/.env.example airbyte-platform/.env
cp dbt-warehouse/.env.example dbt-warehouse/.env
cp airflow-platform/.env.example airflow-platform/.env

# แก้ .env ให้ warehouse ตรงกันทั้ง 3 ไฟล์ (หรือใช้ค่า PoC ด้านบน)
```

---

## ขั้นที่ 0 — สร้าง Docker network (ครั้งเดียว)

```bash
docker network create de_poc_network
```

---

## ขั้นที่ 1 — Source Postgres

```bash
cd source-postgres
docker compose up -d
docker compose ps
```

ตรวจสอบ:

```bash
docker exec de_poc_source_postgres psql -U source_admin -d sales_source -c "SELECT COUNT(*) FROM public.customers;"
```

| จาก | Host | Port |
|-----|------|------|
| DBeaver | `localhost` | `5433` |

---

## ขั้นที่ 2 — Warehouse Postgres

```bash
cd ../warehouse-postgres
docker compose up -d
docker compose ps
```

| จาก | Host | Port |
|-----|------|------|
| DBeaver | `localhost` | `5434` |

---

## ขั้นที่ 3 — Airbyte

```bash
cd ../airbyte-platform
docker compose up -d
```

รอ 2–5 นาที แล้วเปิด [http://localhost:8000](http://localhost:8000)

### ตั้งค่าใน Airbyte UI (ครั้งแรก)

1. **Source** — Postgres  
   - Host: `de_poc_source_postgres`  
   - Port: `5432`  
   - DB: `sales_source`  
   - User / Password: จาก `source-postgres/.env`

2. **Destination** — Postgres  
   - Host: `de_poc_warehouse_postgres`  
   - Port: `5432`  
   - DB: `data_warehouse`  
   - User / Password: จาก `warehouse-postgres/.env`  
   - Namespace: `src_local_postgres`  
   - **Drop tables with CASCADE: OFF**

3. **Connection** `source -> dwh`  
   - Connection ID (สำหรับ Airflow): คัดลอก UUID จาก Airbyte UI → ใส่ `AIRFLOW_VAR_AIRBYTE_CONNECTION_ID` ใน `airflow-platform/.env`
   - Sync mode ต้องเป็น **incremental + append_dedup** (customers: `created_at`, orders: `order_date`) — ดู [PRODUCTION_CHECKLIST.md](PRODUCTION_CHECKLIST.md)  
   - Schedule: **Manual** (ให้ Airflow เป็นคนสั่ง sync)

4. กด **Sync now** ครั้งแรก (หรือรอ Airflow ในขั้น 6)

---

## ขั้นที่ 4 — dbt (แปลง Bronze → Silver → Gold)

```bash
cd ../dbt-warehouse
make build
make deps
make run-full    # ครั้งแรก หรือหลัง Airbyte full reload
make test        # optional
```

ตรวจใน DBeaver (`localhost:5434`):

```sql
SELECT COUNT(*) FROM gold_sales.fct_orders;
SELECT COUNT(*) FROM gold_sales.mart_sales_performance;
```

รันประจำหลัง Airbyte sync (manual):

```bash
make run
```

---

## ขั้นที่ 5 — CKAN (Gold datamart / catalog)

```bash
cd ../ckan-platform
cp .env.example .env
docker compose build
docker compose up -d
# รอจน healthy (~2–3 นาที)
chmod +x scripts/bootstrap-ckan.sh
./scripts/bootstrap-ckan.sh

cd ../airflow-platform
docker compose up -d airflow-scheduler airflow-webserver
```

`bootstrap-ckan.sh` สร้าง token + sync ไป `airflow-platform/.env` ให้อัตโนมัติ (ผ่าน `scripts/patch-ckan-env.sh`) — **ไม่ต้อง copy token มือ** ยกเว้น bootstrap ล้มเหลว

**หลัง `docker compose build ckan` หรือ rebuild CKAN ทุกครั้ง** ให้รันสองคำสั่งด้านบนอีกครั้ง มิฉะนั้น task `publish_gold_to_ckan` จะ fail

เปิด catalog: [http://localhost:5001](http://localhost:5001) — org **ube-group-thailand**, แยก domain Retail / SAP

รายละเอียด: [../ckan-platform/docs/CKAN_SETUP.md](../ckan-platform/docs/CKAN_SETUP.md)

---

## ขั้นที่ 6 — Airflow (orchestration)

```bash
cd ../airflow-platform
mkdir -p logs plugins
sudo chown -R 50000:0 logs plugins   # macOS/Linux ถ้า permission error
docker compose build
docker compose up -d
```

เปิด [http://localhost:8080](http://localhost:8080) — login ตาม `airflow-platform/.env`:

- User: `admin`  
- Password: `admin` (ค่า PoC ปัจจุบัน)

### ตั้งค่า Airflow (ครั้งเดียว)

1. ตรวจ `airflow-platform/.env` มี:
   - `AIRFLOW_VAR_AIRBYTE_CONNECTION_ID=<connection-uuid-from-airbyte-ui>`
   - `AIRFLOW_VAR_AIRBYTE_API_BASE_URL=http://airbyte-proxy:8000/api/v1`
2. `docker compose up -d` ใน `airflow-platform` (ไม่ต้องสร้าง connection `airbyte_default` แล้ว)
3. **DAGs → `elt_main_pipeline` → Unpause → Trigger**

ลำดับใน DAG:

```text
extraction (Airbyte sync)
    → validation (dbt source freshness on `bronze_meta.sync_watermarks`)
    → transformation (run Silver → snapshot → test Silver → run Gold → test Gold)
    → publication (CKAN publish Gold marts → http://localhost:5001)
    → monitoring (log run status — รันแม้ pipeline fail)
```

รายละเอียด best practice: [BEST_PRACTICES_LOCAL.md](./BEST_PRACTICES_LOCAL.md)

**ฝึกดู failure:** [MONITORING_FAILURE_DRILL.md](./MONITORING_FAILURE_DRILL.md)

**แจ้งเตือน email เมื่อ task fail:** [../airflow-platform/docs/AIRFLOW_ALERTING.md](../airflow-platform/docs/AIRFLOW_ALERTING.md) — ดูกล่องจดหมายที่ http://localhost:8025 (Mailpit)

รายละเอียด: [../airflow-platform/docs/AIRFLOW_SETUP.md](../airflow-platform/docs/AIRFLOW_SETUP.md)

---

## สรุป URL / Port

| บริการ | URL |
|--------|-----|
| Airbyte | http://localhost:8000 |
| Airflow | http://localhost:8080 |
| CKAN (Gold datamart) | http://localhost:5001 |
| dbt docs (optional) | http://localhost:8081 (`make docs-serve` ใน dbt-warehouse) |
| Source DB | localhost:5433 |
| Warehouse DB | localhost:5434 |

---

## เลิกงานวันนี้ (ข้อมูลไม่หาย)

```bash
cd airflow-platform && docker compose stop
cd ../ckan-platform && docker compose stop
cd ../airbyte-platform && docker compose stop
cd ../warehouse-postgres && docker compose stop
cd ../source-postgres && docker compose stop
```

อย่าใช้ `docker compose down -v`

---

## วันถัดไป — เปิดใหม่

```bash
cd source-postgres && docker compose start
cd ../warehouse-postgres && docker compose start
cd ../airbyte-platform && docker compose start
cd ../ckan-platform && docker compose start
cd ../airflow-platform && docker compose start
```

จากนั้น Trigger DAG `elt_main_pipeline` หรือรอ schedule **11:00 น. (เวลาไทย)** ทุกวัน

---

## เมื่อมีปัญหา

| อาการ | แก้ |
|--------|-----|
| dbt login failed | ตรวจ `DBT_WAREHOUSE_PASSWORD` ตรงกับ `warehouse-postgres/.env` |
| Airbyte sync failed CASCADE | ปิด CASCADE; ใช้ dbt Silver เป็น table |
| Airflow ไม่เห็น DAG | `docker compose logs airflow-scheduler` |
| Airbyte task ใน Airflow fail ทันที | ดู log: ถ้าเป็น `Invalid URL .../jobs` = ใช้ DAG เวอร์ชัน OSS API; ตรวจ Variables + `airbyte-platform` up |
| Airbyte **409 Conflict** | มี sync รันอยู่แล้ว — DAG รุ่นใหม่จะ **รอ job เดิม**; ดู Airbyte UI หรือ `jobs/list` |
| Airbyte ค้าง running นาน | ปกติสำหรับ ~1M แถว; ดู progress ใน Airbyte UI → Jobs |
| Airflow init error UID | ใช้ `user: 50000:0` (ไม่ใช้ `id -u`) |
| DAG **queued** ไม่จบ | มี run เก่าค้าง `running` + `max_active_runs=1` — ดูด้านล่าง |

### DAG ค้าง queued ไม่รัน

**สาเหตุ:** DAG ตั้ง `max_active_runs=1` แต่มี run เก่า (เช่น `scheduled__2026-05-23`) ค้างสถานะ `running` ทำให้ manual trigger ใหม่ได้แค่ `queued`

**แก้:**

```bash
# 1) ทำให้ run ที่ค้างเป็น failed
docker exec airflow-postgres psql -U airflow -d airflow -c \
  "UPDATE dag_run SET state='failed', end_date=NOW() WHERE dag_id='elt_main_pipeline' AND state IN ('running','queued');"

# 2) restart scheduler
cd airflow-platform
docker compose restart airflow-scheduler

# 3) Trigger DAG ใหม่ใน UI (รอ run เดียว)
```

ถ้า task Airbyte ค้าง `up_for_retry` ให้ตรวจ Variables + ว่า `airbyte-platform` ยัง up อยู่ (sync ~1M แถวอาจใช้เวลานาน — สถานะ running เป็นปกติ)
