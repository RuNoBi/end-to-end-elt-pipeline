#!/usr/bin/env bash
# Idempotent: set CKAN + Airflow env vars for UBE catalog (keeps existing tokens).
set -euo pipefail

set_kv() {
  local file="$1" key="$2" value="$3"
  touch "$file"
  local quoted="${value}"
  if [[ "${value}" == *" "* || "${value}" == *"—"* ]]; then
    quoted="\"${value}\""
  fi
  if grep -q "^${key}=" "$file" 2>/dev/null; then
    sed -i.bak "s|^${key}=.*|${key}=${quoted}|" "$file" && rm -f "${file}.bak"
  else
    echo "${key}=${quoted}" >> "$file"
  fi
}

AF_ROOT="$(cd "$(dirname "$0")/../airflow-platform" && pwd)"
CK_ROOT="$(cd "$(dirname "$0")/../ckan-platform" && pwd)"

AF_ENV="${AF_ROOT}/.env"
CK_ENV="${CK_ROOT}/.env"

for f in "$AF_ENV" "$CK_ENV"; do
  [[ -f "$f" ]] || cp "${f}.example" "$f" 2>/dev/null || touch "$f"
done

# Preserve CKAN_API_TOKEN if already set in ckan .env
if [[ -f "${CK_ENV}" ]] && grep -q '^CKAN_API_TOKEN=' "${CK_ENV}"; then
  TOKEN=$(grep '^CKAN_API_TOKEN=' "${CK_ENV}" | cut -d= -f2-)
  set_kv "$AF_ENV" CKAN_API_TOKEN "$TOKEN"
  set_kv "$AF_ENV" AIRFLOW_VAR_CKAN_API_TOKEN "$TOKEN"
fi

set_kv "$AF_ENV" CKAN_URL "http://ckan:5000"
set_kv "$AF_ENV" CKAN_ORGANIZATION "ube-group-thailand"
set_kv "$AF_ENV" AIRFLOW_VAR_CKAN_ORGANIZATION "ube-group-thailand"
set_kv "$AF_ENV" CKAN_PUBLISH_MAX_ROWS "100000"
set_kv "$AF_ENV" AIRFLOW_VAR_CKAN_PUBLISH_MAX_ROWS "100000"
set_kv "$AF_ENV" CKAN_UPSERT_BATCH_SIZE "2000"
set_kv "$AF_ENV" AIRFLOW_VAR_CKAN_UPSERT_BATCH_SIZE "2000"

set_kv "$CK_ENV" CKAN_ORGANIZATION "ube-group-thailand"
set_kv "$CK_ENV" CKAN_ORGANIZATION_TITLE "UBE Group Thailand"
set_kv "$CK_ENV" CKAN_SITE_TITLE "UBE Group Thailand — Data Catalog"
set_kv "$CK_ENV" CKAN_SITE_DESCRIPTION "Enterprise Gold datamarts from the ELT platform (Airbyte, dbt, Airflow)."

echo "Updated ${AF_ENV} and ${CK_ENV}"
