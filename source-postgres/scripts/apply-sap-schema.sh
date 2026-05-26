#!/usr/bin/env bash
# Apply SAP mock schema to an EXISTING source DB volume (init scripts only run on first start).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="${ROOT}/source-postgres/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE"
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

docker exec -i de_poc_source_postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 \
  < "${ROOT}/source-postgres/init-sap-chemicals.sql"

echo "SAP schema applied on de_poc_source_postgres ($POSTGRES_DB). Tables: sap.sap_*"
