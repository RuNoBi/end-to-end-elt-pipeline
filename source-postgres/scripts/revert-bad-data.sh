#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

if [[ -f "$ROOT_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
fi

: "${POSTGRES_USER:?Set POSTGRES_USER in source-postgres/.env}"
: "${POSTGRES_DB:?Set POSTGRES_DB in source-postgres/.env}"

CONTAINER="${SOURCE_POSTGRES_CONTAINER:-de_poc_source_postgres}"

echo "Reverting drill rows in source ($CONTAINER) ..."
docker exec -i "$CONTAINER" psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < "$SCRIPT_DIR/revert_bad_source_data.sql"

WH_CONTAINER="${WAREHOUSE_POSTGRES_CONTAINER:-de_poc_warehouse_postgres}"
WH_USER="${WAREHOUSE_POSTGRES_USER:-warehouse_admin}"
WH_DB="${WAREHOUSE_POSTGRES_DB:-data_warehouse}"

if docker ps --format '{{.Names}}' | grep -qx "$WH_CONTAINER"; then
  echo "Cleaning drill rows in warehouse Silver/Bronze ($WH_CONTAINER) ..."
  docker exec -i "$WH_CONTAINER" psql -v ON_ERROR_STOP=1 -U "$WH_USER" -d "$WH_DB" \
    < "$SCRIPT_DIR/revert_warehouse_drill.sql"
else
  echo "WARN: warehouse container $WH_CONTAINER not running — run revert again after warehouse is up."
fi

echo "Done. Trigger elt_main_pipeline (or Clear + rerun transformation tasks)."
