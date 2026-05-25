#!/usr/bin/env bash
# Send a test email using Airflow SMTP settings (verify .env before relying on alerts).
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose exec -T airflow-scheduler bash -lc 'python3 <<PY
import os
import sys

sys.path.insert(0, "/opt/airflow/dags")
from common.alerting import get_alert_recipients

recipients = get_alert_recipients()
if not recipients:
    print("ERROR: Set AIRFLOW_ALERT_EMAILS in airflow-platform/.env")
    raise SystemExit(1)

host = os.environ.get("AIRFLOW_SMTP_HOST") or os.environ.get("AIRFLOW__SMTP__SMTP_HOST")
if not host:
    print("ERROR: Set AIRFLOW_SMTP_HOST in .env")
    raise SystemExit(1)

from airflow.utils.email import send_email

send_email(
    to=recipients,
    subject="[ELT ALERT][TEST] Airflow SMTP configuration OK",
    html_content=(
        "<p>If you see this in <b>Mailpit</b>, failure alerts for "
        "<code>elt_main_pipeline</code> are wired correctly.</p>"
    ),
)
print("OK: test email sent to", recipients)
print("Open Mailpit inbox: http://localhost:8025")
PY'
