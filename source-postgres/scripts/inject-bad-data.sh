#!/usr/bin/env bash
# Inject bad source rows for ELT failure drill (see docs/MONITORING_FAILURE_DRILL.md).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
MODE="${1:-orphan_order}"

if [[ -f "$ROOT_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
fi

: "${POSTGRES_USER:?Set POSTGRES_USER in source-postgres/.env}"
: "${POSTGRES_DB:?Set POSTGRES_DB in source-postgres/.env}"

CONTAINER="${SOURCE_POSTGRES_CONTAINER:-de_poc_source_postgres}"

run_sql() {
  local file="$1"
  echo "Running $(basename "$file") ..."
  docker exec -i "$CONTAINER" psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" < "$file"
}

case "$MODE" in
  orphan_order)
    run_sql "$SCRIPT_DIR/inject_orphan_order.sql"
    ;;
  duplicate_email)
    run_sql "$SCRIPT_DIR/inject_duplicate_email.sql"
    ;;
  both)
    run_sql "$SCRIPT_DIR/inject_orphan_order.sql"
    run_sql "$SCRIPT_DIR/inject_duplicate_email.sql"
    ;;
  *)
    echo "Usage: $0 [orphan_order|duplicate_email|both]"
    exit 1
    ;;
esac

echo ""
echo "Injected (mode=$MODE). Trigger elt_main_pipeline in Airflow, then check Grid + Logs."
echo "Revert: $SCRIPT_DIR/revert-bad-data.sh"
