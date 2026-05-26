#!/usr/bin/env bash
# Daily stop: keep volumes, stop services cleanly.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "Stopping CKAN..."
(cd ckan-platform && docker compose stop)

echo "Stopping Airflow..."
(cd airflow-platform && docker compose stop)

echo "Stopping Airbyte..."
(cd airbyte-platform && docker compose stop)

echo "Stopping Warehouse Postgres..."
(cd warehouse-postgres && docker compose stop)

echo "Stopping Source Postgres..."
(cd source-postgres && docker compose stop)

echo "Done."
