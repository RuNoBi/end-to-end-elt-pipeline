#!/usr/bin/env bash
# Daily start: avoid DB recreation; bring services up in safe order.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# SAP mock is schema sap on source-postgres — remove obsolete separate container if it still exists
if docker ps -aq --filter name=^de_poc_source_sap_chemicals$ 2>/dev/null | grep -q .; then
  echo "Removing legacy de_poc_source_sap_chemicals (use source-postgres only)..."
  docker rm -f de_poc_source_sap_chemicals >/dev/null 2>&1 || true
fi

echo "Starting Source Postgres (retail public.* + SAP schema sap.*)..."
(cd source-postgres && docker compose up -d --no-recreate)

echo "Starting Warehouse Postgres..."
(cd warehouse-postgres && docker compose up -d --no-recreate)

echo "Starting Airbyte..."
(cd airbyte-platform && docker compose up -d --no-recreate)

echo "Starting Airflow..."
(cd airflow-platform && docker compose up -d --no-recreate airflow-scheduler airflow-webserver)

echo "Starting CKAN..."
(cd ckan-platform && docker compose up -d --no-recreate)

echo "Done."
