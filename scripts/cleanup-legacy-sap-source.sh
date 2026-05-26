#!/usr/bin/env bash
# Remove obsolete de_poc_source_sap_chemicals (SAP mock now lives in source-postgres / schema sap).
set -euo pipefail

NAME="de_poc_source_sap_chemicals"
VOLUME="source_sap_chemicals_data"

if docker ps -aq --filter "name=^${NAME}$" 2>/dev/null | grep -q .; then
  echo "Removing container ${NAME}..."
  docker rm -f "${NAME}" >/dev/null 2>&1 || true
else
  echo "Container ${NAME} not present."
fi

if docker volume ls -q --filter "name=^${VOLUME}$" 2>/dev/null | grep -q .; then
  read -r -p "Delete unused volume ${VOLUME}? Data was moved to source-postgres. [y/N] " ans
  if [[ "${ans}" =~ ^[Yy]$ ]]; then
    docker volume rm "${VOLUME}" >/dev/null 2>&1 && echo "Volume removed." || echo "Volume in use or not removed."
  else
    echo "Keeping volume (safe to delete manually later)."
  fi
fi

echo "Done. Use only: source-postgres (port 5433) with schema sap.*"
