#!/usr/bin/env bash
# Read-only checks before treating the stack as production-ready (no secrets required).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FAIL=0

ok() { echo "OK   $*"; }
warn() { echo "WARN $*"; }
fail() { echo "FAIL $*"; FAIL=1; }

AIRBYTE_API="${AIRBYTE_API:-http://localhost:8000/api/v1}"
CONN_ID="${AIRBYTE_CONNECTION_ID:-}"

if [[ -f "${ROOT}/airflow-platform/.env" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/airflow-platform/.env"
  CONN_ID="${CONN_ID:-${AIRFLOW_VAR_AIRBYTE_CONNECTION_ID:-}}"
fi

echo "=== Airbyte connection (incremental) ==="
if [[ -z "${CONN_ID}" ]]; then
  fail "AIRFLOW_VAR_AIRBYTE_CONNECTION_ID not set in airflow-platform/.env"
else
  if curl -sf -X POST "${AIRBYTE_API}/connections/get" \
    -H "Content-Type: application/json" \
    -d "{\"connectionId\":\"${CONN_ID}\"}" \
    | python3 -c "
import json, sys
d = json.load(sys.stdin)
streams = (d.get('syncCatalog') or {}).get('streams') or []
bad = []
for s in streams:
    c = s.get('config') or {}
    if not c.get('selected', True):
        continue
    name = (s.get('stream') or {}).get('name')
    if c.get('syncMode') != 'incremental' or c.get('destinationSyncMode') != 'append_dedup':
        bad.append(name)
if bad:
    print('NON_INCREMENTAL:', ','.join(bad))
    sys.exit(1)
print('streams:', ','.join(
    (s.get('stream') or {}).get('name', '?')
    for s in streams if (s.get('config') or {}).get('selected', True)
))
"; then
    ok "connection ${CONN_ID} uses incremental + append_dedup"
  else
    fail "connection ${CONN_ID} has full_refresh or wrong destination mode"
  fi
fi

echo ""
echo "=== Docker services (optional) ==="
for svc in de_poc_source_postgres de_poc_warehouse_postgres; do
  if docker ps --format '{{.Names}}' | grep -qx "${svc}"; then
    ok "container ${svc} running"
  else
    warn "container ${svc} not running (start stack for E2E)"
  fi
done

echo ""
echo "=== Repo hygiene ==="
if rg -q 'AIRBYTE_SKIP|skip_if_bronze' "${ROOT}" 2>/dev/null; then
  fail "demo skip logic still referenced in repo"
else
  ok "no Bronze skip-by-row-count references"
fi

if [[ "${FAIL}" -eq 0 ]]; then
  echo ""
  echo "Pipeline config checks passed. Run Airflow DAG elt_main_pipeline for full E2E."
  exit 0
fi
echo ""
echo "Fix failures above, then re-run: ./scripts/verify-pipeline-ready.sh"
exit 1
