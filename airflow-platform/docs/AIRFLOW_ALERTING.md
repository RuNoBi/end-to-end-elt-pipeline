# Airflow failure email alerts

Production-style pattern: **one email per task that permanently fails** (after retries), with DAG/task/run context and a link to logs.

Alerts apply to every task in `elt_main_pipeline` via `default_args.on_failure_callback`.

---

## 1. Already configured (Mailpit — no Gmail password needed)

This repo ships with **Mailpit** SMTP inside Docker:

| Item | Value |
|------|--------|
| SMTP host (from Airflow) | `airflow-mailpit:1025` |
| Web inbox | **http://localhost:8025** |
| Alert recipient | `AIRFLOW_ALERT_EMAILS` from `airflow-platform/.env` (shown in Mailpit UI) |

Start / restart:

```bash
cd airflow-platform
docker compose up -d
./scripts/test_smtp_alert.sh
```

Open **http://localhost:8025** — you should see the test message.

---

## 2. Forward to real `@student.chula.ac.th` inbox (optional)

Mailpit captures mail locally. To **deliver to Gmail/Chula inbox**, add a Gmail **App Password** to `.env`:

```bash
AIRFLOW_SMTP_RELAY_HOST=smtp.gmail.com
AIRFLOW_SMTP_RELAY_PORT=587
AIRFLOW_SMTP_RELAY_STARTTLS=True
AIRFLOW_SMTP_RELAY_AUTH=plain
AIRFLOW_SMTP_RELAY_USER=your@gmail.com
AIRFLOW_SMTP_RELAY_PASSWORD=your_16_char_app_password
AIRFLOW_SMTP_RELAY_ALL=true
```

Then:

```bash
./scripts/enable-gmail-relay.sh
docker compose up -d airflow-mailpit airflow-scheduler
./scripts/test_smtp_alert.sh
```

---

## 3. Manual SMTP (skip Mailpit)

Edit `airflow-platform/.env` (never commit):

```bash
AIRFLOW_ALERT_EMAILS=your_oncall_list@example.com
AIRFLOW_WEBSERVER_BASE_URL=http://localhost:8080
AIRFLOW_SMTP_HOST=smtp.gmail.com
AIRFLOW_SMTP_PORT=587
AIRFLOW_SMTP_USER=your_login@gmail.com
AIRFLOW_SMTP_PASSWORD=your_app_password_here
AIRFLOW_SMTP_MAIL_FROM=your_login@gmail.com
AIRFLOW_SMTP_STARTTLS=True
AIRFLOW_SMTP_SSL=False
```

Restart Airflow after changes:

```bash
cd airflow-platform
docker compose up -d airflow-scheduler airflow-webserver
```

---

## 4. SMTP options for `@student.chula.ac.th`

| Provider | Host | Port | Notes |
|----------|------|------|--------|
| **Gmail** (if Chula uses Google) | `smtp.gmail.com` | 587 | Enable 2FA → [App Password](https://myaccount.google.com/apppasswords) |
| **Microsoft 365** | `smtp.office365.com` | 587 | University login; may need IT to allow SMTP AUTH |
| **Portfolio / dev** | [Brevo](https://www.brevo.com/) / [Mailtrap](https://mailtrap.io/) | 587 | Free tier; good for demos without personal inbox |

Use the **same mailbox** for `SMTP_USER`, `SMTP_PASSWORD`, and `SMTP_MAIL_FROM` unless your provider specifies a relay address.

---

## 5. Test delivery (before waiting for a real failure)

```bash
cd airflow-platform
chmod +x scripts/test_smtp_alert.sh
./scripts/test_smtp_alert.sh
```

Check inbox (and spam). If this fails, fix SMTP before triggering the DAG.

---

## 6. What you get on failure

Example subject:

```text
[ELT ALERT][FAILED] elt_main_pipeline · transformation.dbt_test_silver · try 2/2
```

Body includes:

- DAG, task, run id, logical date  
- **Triage hint** (Silver gate vs Airbyte vs Gold)  
- **Root cause + immediate actions** (rule-based from logs)  
- **Violating rows** (live warehouse preview when available)  
- Compiled test SQL + dbt failure log block  
- Link to **Grid → Logs**

**Debug checklist:** [docs/FAILURE_EMAIL_DEBUG_CHECKLIST.md](../../docs/FAILURE_EMAIL_DEBUG_CHECKLIST.md)

Retries: with `retries: 1`, you get **at most one email per failed task** when the last try fails — not on the first retry.

---

## 7. Senior DE practices (included in this PoC)

| Practice | How this repo does it |
|----------|------------------------|
| Alert **after retries** | Airflow `on_failure_callback` on final failure only |
| No secrets in Git | SMTP password only in `.env` |
| Actionable subject | `[ELT ALERT][FAILED]` + dag · task |
| Layer-aware hints | `dags/common/alerting.py` `LAYER_HINTS` |
| Rule-based triage from logs | `dags/common/alerting.py` parses task logs for dbt/Airbyte hints |
| Test channel | `scripts/test_smtp_alert.sh` |
| Don’t alert on retry noise | `email_on_retry: False` |

**Roadmap (real production):** PagerDuty/Opsgenie, Slack webhook, deduplicated alerts per DAG run, `#data-alerts` with runbook links, SLO on `dbt_test_silver` duration.

---

## 8. Troubleshooting

| Symptom | Fix |
|---------|-----|
| No email, log says `AIRFLOW_ALERT_EMAILS is empty` | Set `AIRFLOW_ALERT_EMAILS` in `.env` and restart |
| `SMTP credentials not provided` | Set `AIRFLOW_SMTP_*` in `.env` |
| Gmail “Less secure” / auth failed | Use **App Password**, not normal password |
| Email in spam | Add sender to contacts; use consistent `SMTP_MAIL_FROM` |
| Link in email 404 | Set `AIRFLOW_WEBSERVER_BASE_URL` to URL you actually use |
