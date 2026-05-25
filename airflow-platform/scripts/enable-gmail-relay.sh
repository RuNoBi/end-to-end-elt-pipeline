#!/usr/bin/env bash
# Forward Mailpit messages to a real Gmail inbox (requires App Password in .env).
set -euo pipefail

ENV_FILE="$(dirname "$0")/../.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE"
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

if [[ -z "${AIRFLOW_SMTP_RELAY_USER:-}" || -z "${AIRFLOW_SMTP_RELAY_PASSWORD:-}" ]]; then
  cat <<'EOF'
Add to airflow-platform/.env (Gmail App Password):

  AIRFLOW_SMTP_RELAY_HOST=smtp.gmail.com
  AIRFLOW_SMTP_RELAY_PORT=587
  AIRFLOW_SMTP_RELAY_STARTTLS=True
  AIRFLOW_SMTP_RELAY_AUTH=plain
  AIRFLOW_SMTP_RELAY_USER=your@gmail.com
  AIRFLOW_SMTP_RELAY_PASSWORD=xxxx xxxx xxxx xxxx
  AIRFLOW_SMTP_RELAY_ALL=true

Then run this script again.
EOF
  exit 1
fi

cd "$(dirname "$0")/.."
docker compose up -d airflow-mailpit
echo "Gmail relay enabled. Restart scheduler and run ./scripts/test_smtp_alert.sh"
