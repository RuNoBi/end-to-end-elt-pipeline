#!/usr/bin/env bash
# One-time / after-reset CKAN bootstrap: org, API token, datastore permissions, UBE catalog.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CLI_ORG="${CKAN_ORGANIZATION:-}"
if [[ -f .env ]]; then
  # shellcheck disable=SC1091
  source .env
fi

CKAN_URL="${CKAN_SITE_URL:-http://localhost:5001}"
ADMIN="${CKAN_SYSADMIN_NAME:-ckan_admin}"
ORG="${CLI_ORG:-${CKAN_ORGANIZATION:-ube-group-thailand}}"
ORG_TITLE="${CKAN_ORGANIZATION_TITLE:-UBE Group Thailand}"

echo "Waiting for CKAN at ${CKAN_URL} ..."
for i in $(seq 1 60); do
  if curl -sf "${CKAN_URL}/api/action/status_show" >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

echo "Setting datastore permissions (run SQL on ckan-db) ..."
docker compose exec -T ckan ckan -c /srv/app/ckan.ini datastore set-permissions 2>/dev/null \
  | docker compose exec -T ckan-db psql -U postgres -d datastore -v ON_ERROR_STOP=1 \
  || echo "WARN: datastore permissions may already be applied."

token_works() {
  local token="$1"
  [[ -z "$token" ]] && return 1
  # Must be able to edit the org (same permission Airflow needs for package_create).
  curl -sf -X POST "${CKAN_URL}/api/3/action/organization_patch" \
    -H "Authorization: ${token}" \
    -H "Content-Type: application/json" \
    -d "{\"id\":\"${ORG}\",\"notes\":\"token_probe\"}" \
    >/dev/null 2>&1
}

# Prefer reusing existing token to avoid churn.
TOKEN="${CKAN_API_TOKEN:-}"
if token_works "${TOKEN}"; then
  echo "Reusing existing CKAN_API_TOKEN (write access ok)."
else
  echo "Creating API token for Airflow (user=${ADMIN}) ..."
  TOKEN=$(docker compose exec -T ckan ckan -c /srv/app/ckan.ini user token add "${ADMIN}" elt-pipeline 2>/dev/null \
    | awk '/^API Token created:/{getline; print $1}' | tr -d '[:space:]')

  if [[ -z "${TOKEN}" ]]; then
    echo "ERROR: could not create API token. Check: docker compose logs ckan"
    exit 1
  fi
fi

# Persist for local reference (git-ignored .env)
if grep -q '^CKAN_API_TOKEN=' .env 2>/dev/null; then
  sed -i.bak "s|^CKAN_API_TOKEN=.*|CKAN_API_TOKEN=${TOKEN}|" .env && rm -f .env.bak
else
  echo "CKAN_API_TOKEN=${TOKEN}" >> .env
fi

export CKAN_API_TOKEN="${TOKEN}"
"${ROOT}/scripts/configure-ube-catalog.sh"

# Keep Airflow in sync (stale token after ckan-db recreate / bootstrap is the #1 publish failure).
PATCH_SCRIPT="$(cd "${ROOT}/../scripts" && pwd)/patch-ckan-env.sh"
if [[ -x "${PATCH_SCRIPT}" ]]; then
  echo "Syncing token to airflow-platform/.env ..."
  "${PATCH_SCRIPT}"
fi

echo ""
echo "=== Restart Airflow (pick up new token) ==="
echo "cd airflow-platform && docker compose up -d airflow-scheduler airflow-webserver"
echo ""
echo "=== airflow-platform/.env (synced) ==="
echo "CKAN_URL=http://ckan:5000"
echo "CKAN_API_TOKEN=${TOKEN}"
echo "AIRFLOW_VAR_CKAN_API_TOKEN=${TOKEN}"
echo "CKAN_ORGANIZATION=${ORG}"
echo "AIRFLOW_VAR_CKAN_ORGANIZATION=${ORG}"
echo "CKAN_PUBLISH_MAX_ROWS=100000"
echo "AIRFLOW_VAR_CKAN_PUBLISH_MAX_ROWS=100000"
echo "CKAN_UPSERT_BATCH_SIZE=2000"
echo "AIRFLOW_VAR_CKAN_UPSERT_BATCH_SIZE=2000"
echo "=================================="
echo "Catalog UI: ${CKAN_URL}"
echo "Organization: ${ORG_TITLE} (${ORG})"
