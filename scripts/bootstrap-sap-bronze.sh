#!/usr/bin/env bash
# Land sap.* tables from the shared source DB into warehouse Bronze (src_sap_chemicals).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE_ENV="${ROOT}/source-postgres/.env"
WAREHOUSE_ENV="${ROOT}/warehouse-postgres/.env"

if [[ ! -f "$SOURCE_ENV" ]] || [[ ! -f "$WAREHOUSE_ENV" ]]; then
  echo "Missing source-postgres/.env or warehouse-postgres/.env"
  exit 1
fi

# shellcheck disable=SC1090
source "$SOURCE_ENV"
SRC_USER="$POSTGRES_USER"
SRC_PASS="$POSTGRES_PASSWORD"
SRC_DB="$POSTGRES_DB"

# shellcheck disable=SC1090
source "$WAREHOUSE_ENV"
WH_USER="$POSTGRES_USER"
WH_PASS="$POSTGRES_PASSWORD"
WH_DB="$POSTGRES_DB"

echo "Starting shared source + warehouse..."
(cd "$ROOT/source-postgres" && docker compose up -d --no-recreate)

if ! docker exec de_poc_source_postgres psql -U "$SRC_USER" -d "$SRC_DB" -tAc \
  "SELECT 1 FROM information_schema.tables WHERE table_schema='sap' AND table_name='sap_material' LIMIT 1" \
  | grep -q 1; then
  echo "SAP schema missing — applying init-sap-chemicals.sql on source..."
  chmod +x "${ROOT}/source-postgres/scripts/apply-sap-schema.sh"
  "${ROOT}/source-postgres/scripts/apply-sap-schema.sh"
fi

(cd "$ROOT/warehouse-postgres" && docker compose up -d --no-recreate)

until docker exec de_poc_source_postgres pg_isready -U "$SRC_USER" -d "$SRC_DB" >/dev/null 2>&1; do sleep 1; done
until docker exec de_poc_warehouse_postgres pg_isready -U "$WH_USER" -d "$WH_DB" >/dev/null 2>&1; do sleep 1; done

export PGPASSWORD="$WH_PASS"

docker exec -i de_poc_warehouse_postgres psql -U "$WH_USER" -d "$WH_DB" -v ON_ERROR_STOP=1 <<EOSQL
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SCHEMA IF EXISTS sap_fdw_import CASCADE;
DROP SERVER IF EXISTS shared_source CASCADE;

CREATE SERVER shared_source
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'de_poc_source_postgres', port '5432', dbname '${SRC_DB}');

CREATE USER MAPPING FOR ${WH_USER}
  SERVER shared_source
  OPTIONS (user '${SRC_USER}', password '${SRC_PASS}');

CREATE SCHEMA sap_fdw_import;

IMPORT FOREIGN SCHEMA sap
  LIMIT TO (sap_material, sap_business_partner, sap_sales_order, sap_sales_order_item)
  FROM SERVER shared_source
  INTO sap_fdw_import;

CREATE SCHEMA IF NOT EXISTS src_sap_chemicals;

DROP TABLE IF EXISTS src_sap_chemicals.sap_material CASCADE;
CREATE TABLE src_sap_chemicals.sap_material AS
SELECT m.*, gen_random_uuid()::text AS _airbyte_raw_id, NOW() AS _airbyte_extracted_at, '{}'::jsonb AS _airbyte_meta
FROM sap_fdw_import.sap_material AS m;

DROP TABLE IF EXISTS src_sap_chemicals.sap_business_partner CASCADE;
CREATE TABLE src_sap_chemicals.sap_business_partner AS
SELECT b.*, gen_random_uuid()::text AS _airbyte_raw_id, NOW() AS _airbyte_extracted_at, '{}'::jsonb AS _airbyte_meta
FROM sap_fdw_import.sap_business_partner AS b;

DROP TABLE IF EXISTS src_sap_chemicals.sap_sales_order CASCADE;
CREATE TABLE src_sap_chemicals.sap_sales_order AS
SELECT o.*, gen_random_uuid()::text AS _airbyte_raw_id, NOW() AS _airbyte_extracted_at, '{}'::jsonb AS _airbyte_meta
FROM sap_fdw_import.sap_sales_order AS o;

DROP TABLE IF EXISTS src_sap_chemicals.sap_sales_order_item CASCADE;
CREATE TABLE src_sap_chemicals.sap_sales_order_item AS
SELECT i.*, gen_random_uuid()::text AS _airbyte_raw_id, NOW() AS _airbyte_extracted_at, '{}'::jsonb AS _airbyte_meta
FROM sap_fdw_import.sap_sales_order_item AS i;

DROP SCHEMA IF EXISTS sap_fdw_import CASCADE;
DROP SERVER IF EXISTS shared_source CASCADE;
EOSQL

echo "Bronze ready: src_sap_chemicals (from shared source-postgres / schema sap)."
