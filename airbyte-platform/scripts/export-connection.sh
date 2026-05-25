#!/usr/bin/env bash
# Export Airbyte connection JSON for config-as-code (OSS 0.63 API).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONN_ID="${1:-${AIRFLOW_VAR_AIRBYTE_CONNECTION_ID:-}}"
API_BASE="${AIRBYTE_API_BASE_URL:-http://localhost:8000/api/v1}"
OUT_DIR="$ROOT/config/connections"
mkdir -p "$OUT_DIR"

if [[ -z "$CONN_ID" ]]; then
  echo "Usage: AIRFLOW_VAR_AIRBYTE_CONNECTION_ID=<uuid> $0"
  echo "   or: $0 <connection-uuid>"
  exit 1
fi

OUT_FILE="$OUT_DIR/source_to_dwh_${CONN_ID}.json"
echo "Exporting connection $CONN_ID from $API_BASE ..."
curl -sf -X POST "$API_BASE/connections/get" \
  -H "Content-Type: application/json" \
  -d "{\"connectionId\": \"$CONN_ID\"}" \
  | python3 -m json.tool > "$OUT_FILE"

echo "Wrote $OUT_FILE"
