#!/usr/bin/env bash
# Pull Docker images required for Connector Builder (declarative manifest) sources.
set -euo pipefail

TAG="${1:-4.6.2}"
IMAGE="airbyte/source-declarative-manifest:${TAG}"

echo "Pulling ${IMAGE} ..."
docker pull "${IMAGE}"

echo "Restarting Airbyte worker + docker-proxy so sync/check sees the image ..."
cd "$(dirname "$0")/.."
docker compose restart worker docker-proxy

echo "Done. Retry 'Test the source' in Airbyte UI."
