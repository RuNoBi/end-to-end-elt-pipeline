"""
Airflow failure alerting — email on task failure (after retries exhausted).

Evidence-only triage: parse task logs, read dbt compiled SQL, optionally query
the warehouse for sample violating rows. No generic drill templates.
"""

from __future__ import annotations

import html
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
AIRFLOW_LOG_PREFIX_RE = re.compile(
    r"^\[[^\]]+\]\s+\{[^}]+\}\s+\w+\s+-\s+(.*)$"
)
UNSAFE_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|grant|revoke|create)\b",
    re.IGNORECASE,
)
DBT_FAIL_BLOCK_RE = re.compile(
    r"Failure in test\s+(.+?)\s+\(([^)]+)\)\s*\n\s*Got\s+([^\n]+)"
    r"(?:\s*\n\s*compiled code at\s+(\S+))?",
    re.DOTALL,
)
CHILD_TABLE_RE = re.compile(
    r'from\s+"[^"]+"\."([^"]+)"\."([^"]+)"',
    re.IGNORECASE,
)
CHILD_COLUMN_RE = re.compile(
    r"select\s+(\w+)\s+as\s+from_field",
    re.IGNORECASE,
)


@dataclass
class FailureInsight:
    root_cause: str
    steps: list[str] = field(default_factory=list)
    test_name: str = ""
    test_resource: str = ""
    test_type: str = ""
    result_line: str = ""
    compiled_sql_path: str = ""
    compiled_sql_snippet: str = ""
    log_highlights: str = ""
    dbt_summary: str = ""
    violation_rows: list[dict[str, Any]] = field(default_factory=list)
    violation_table: str = ""
    violation_query_note: str = ""
    child_column: str = ""


def get_alert_recipients() -> list[str]:
    raw = os.environ.get("AIRFLOW_ALERT_EMAILS", "")
    return [addr.strip() for addr in raw.split(",") if addr.strip()]


def _strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def _unwrap_airflow_log_lines(raw: str) -> str:
    out: list[str] = []
    for line in raw.splitlines():
        m = AIRFLOW_LOG_PREFIX_RE.match(line)
        payload = _strip_ansi(m.group(1) if m else line).strip()
        if payload and not (payload.startswith("::") and "group::" in payload):
            out.append(payload)
    return "\n".join(out)


def _log_candidates(ti: Any) -> list[Path]:
    base_logs = Path(os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs"))
    run_id = str(getattr(ti, "run_id", "") or "")
    task_id = str(ti.task_id)
    dag_id = str(ti.dag_id)
    try_number = int(getattr(ti, "try_number", 1) or 1)

    candidates: list[Path] = []
    if getattr(ti, "log_filepath", None):
        candidates.append(Path(str(ti.log_filepath)))

    for attempt in range(try_number, 0, -1):
        if run_id:
            candidates.append(
                base_logs
                / f"dag_id={dag_id}"
                / f"run_id={run_id}"
                / f"task_id={task_id}"
                / f"attempt={attempt}.log"
            )
    return candidates


def _collect_log_text(context: dict[str, Any], max_chars: int = 120_000) -> str:
    ti = context["task_instance"]
    for candidate in _log_candidates(ti):
        try:
            if candidate.is_file():
                return _unwrap_airflow_log_lines(
                    candidate.read_text(encoding="utf-8", errors="replace")
                )[-max_chars:]
        except Exception as exc:
            logger.warning("Could not read log %s: %s", candidate, exc)
    return ""


def _resolve_compiled_sql(rel_path: str) -> Path | None:
    for base in (Path("/opt/dbt"), Path(".")):
        path = (base / rel_path).resolve()
        try:
            if path.is_file() and str(path).startswith(str(base.resolve())):
                return path
        except Exception:
            continue
    return None


def _read_compiled_sql(rel_path: str, max_lines: int | None = 30) -> tuple[str, str]:
    path = _resolve_compiled_sql(rel_path)
    if not path:
        return rel_path, ""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if max_lines is None:
        return str(path), "\n".join(lines)
    snippet = "\n".join(lines[:max_lines])
    if len(lines) > max_lines:
        snippet += "\n-- ... (truncated)"
    return str(path), snippet


def _is_safe_readonly_sql(sql: str) -> bool:
    stripped = sql.strip().rstrip(";")
    if UNSAFE_SQL_RE.search(stripped):
        return False
    lowered = stripped.lower()
    return lowered.startswith("select") or lowered.startswith("with")


def _append_limit(sql: str, limit: int) -> str:
    body = sql.strip().rstrip(";")
    if re.search(r"\blimit\b", body, re.IGNORECASE):
        return body
    return f"{body}\nLIMIT {limit}"


def _parse_child_table(sql: str) -> str:
    m = CHILD_TABLE_RE.search(sql)
    if not m:
        return ""
    return f"{m.group(1)}.{m.group(2)}"


def _parse_child_column(sql: str) -> str:
    m = CHILD_COLUMN_RE.search(sql)
    return m.group(1) if m else "from_field"


def _fetch_violation_rows(compiled_rel_path: str, limit: int = 5) -> tuple[list[dict[str, Any]], str, str]:
    """Run dbt compiled test SQL; return (rows, child_table, note)."""
    path = _resolve_compiled_sql(compiled_rel_path)
    if not path:
        return [], "", "compiled SQL file not found on disk"

    sql = path.read_text(encoding="utf-8", errors="replace")
    if not _is_safe_readonly_sql(sql):
        return [], _parse_child_table(sql), "auto-preview skipped (not a read-only SELECT)"

    child_table = _parse_child_table(sql)
    query = _append_limit(sql, limit)

    host = os.environ.get("DBT_WAREHOUSE_HOST")
    user = os.environ.get("DBT_WAREHOUSE_USER")
    password = os.environ.get("DBT_WAREHOUSE_PASSWORD")
    dbname = os.environ.get("DBT_WAREHOUSE_DB")
    port = os.environ.get("DBT_WAREHOUSE_PORT", "5432")
    if not all([host, user, password, dbname]):
        return [], child_table, "warehouse credentials not set in scheduler env"

    try:
        import psycopg2
        import psycopg2.extras

        conn = psycopg2.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            dbname=dbname,
            connect_timeout=5,
        )
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                rows = [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
        return rows, child_table, ""
    except Exception as exc:
        logger.warning("Violation preview query failed: %s", exc)
        return [], child_table, f"could not run compiled SQL: {exc}"


def _infer_test_type(test_name: str) -> str:
    n = test_name.lower()
    if "relationships" in n or "relationship" in n:
        return "relationships"
    if "unique" in n:
        return "unique"
    if "not_null" in n:
        return "not_null"
    return "generic"


def _parse_dbt_failure(log_text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if not log_text:
        return out

    block = DBT_FAIL_BLOCK_RE.search(log_text)
    if block:
        out["test_name"] = block.group(1).strip()
        out["test_resource"] = block.group(2).strip()
        out["result_line"] = block.group(3).strip()
        if block.group(4):
            out["compiled_sql_path"] = block.group(4).strip()
    else:
        m = re.search(r"Failure in test\s+(.+?)\s+\(([^)]+)\)", log_text)
        if m:
            out["test_name"] = m.group(1).strip()
            out["test_resource"] = m.group(2).strip()
        got = re.search(r"Got\s+(\d+)\s+result[s]?,[^\n]+", log_text)
        if got:
            out["result_line"] = got.group(0).strip()
        compiled = re.search(r"compiled code at\s+(\S+)", log_text)
        if compiled:
            out["compiled_sql_path"] = compiled.group(1).strip()

    summary = re.search(
        r"Done\.\s+PASS=(\d+)\s+WARN=(\d+)\s+ERROR=(\d+)\s+SKIP=(\d+)\s+TOTAL=(\d+)",
        log_text,
    )
    if summary:
        out["dbt_summary"] = (
            f"PASS={summary.group(1)} ERROR={summary.group(3)} "
            f"TOTAL={summary.group(5)}"
        )
    return out


def _extract_failure_log_slice(log_text: str) -> str:
    """Only the dbt failure block — no Airflow traceback noise."""
    if not log_text:
        return ""
    start = log_text.find("Failure in test")
    if start < 0:
        return _extract_log_highlights(log_text, max_lines=12)
    end = log_text.find("Done. PASS=", start)
    if end < 0:
        end = log_text.find("Command exited with return code", start)
    if end < 0:
        end = start + 2500
    return log_text[start:end].strip()


def _extract_log_highlights(log_text: str, max_lines: int = 12) -> str:
    keywords = ("failure in test", " fail ", "got ", "compiled code at", "done. pass=", "database error")
    picked = [
        ln.strip()
        for ln in log_text.splitlines()
        if any(k in ln.lower() for k in keywords)
    ]
    return "\n".join(picked[-max_lines:])


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _delete_hint(table: str, rows: list[dict[str, Any]], child_column: str = "") -> str | None:
    """DELETE statement from actual preview rows only."""
    if not table or not rows:
        return None
    row = rows[0]
    col = child_column
    if "from_field" in row and col:
        values = [_sql_literal(r["from_field"]) for r in rows if r.get("from_field") is not None]
        if values:
            return f"DELETE FROM {table} WHERE {col} IN ({', '.join(values)});"
    for pk in ("sales_order_number", "id", "order_id", "material_number", "partner_number"):
        if pk in row:
            values = [_sql_literal(r[pk]) for r in rows if r.get(pk) is not None]
            if values:
                return f"DELETE FROM {table} WHERE {pk} IN ({', '.join(values)});"
    return None


def _build_insight(task_id: str, exception: Any, log_text: str) -> FailureInsight:
    parsed = _parse_dbt_failure(log_text)
    test_name = parsed.get("test_name", "")
    test_resource = parsed.get("test_resource", "")
    test_type = _infer_test_type(test_name) if test_name else ""
    result_line = parsed.get("result_line", "")
    compiled_rel = parsed.get("compiled_sql_path", "")
    compiled_abs, compiled_snippet = "", ""
    violation_rows: list[dict[str, Any]] = []
    violation_table = ""
    violation_note = ""
    child_column = ""

    if compiled_rel:
        compiled_abs, compiled_snippet = _read_compiled_sql(compiled_rel, max_lines=35)
        full_sql = _read_compiled_sql(compiled_rel, max_lines=None)[1]
        child_column = _parse_child_column(full_sql)
        if test_name:
            violation_rows, violation_table, violation_note = _fetch_violation_rows(
                compiled_rel, limit=5
            )

    log_slice = _extract_failure_log_slice(log_text)
    dbt_summary = parsed.get("dbt_summary", "")
    steps: list[str] = []

    if test_name:
        root_cause = test_name
        if result_line:
            root_cause = f"{test_name} — {result_line}"
        if violation_rows:
            sample = ", ".join(
                f"{k}={v}" for k, v in list(violation_rows[0].items())[:4]
            )
            root_cause += f" — violating key in `{violation_table}`: {sample}"
        elif violation_note:
            root_cause += f" — ({violation_note})"
        elif result_line and re.search(r"Got\s+[1-9]", result_line):
            root_cause += f" — check `{violation_table or compiled_rel}` (preview returned 0 rows now)"

        steps.append(f"Re-run: `dbt test --select {test_name}`")

        delete_sql = _delete_hint(violation_table, violation_rows, child_column)
        if delete_sql:
            steps.append(f"Remove invalid row(s): `{delete_sql}`")
        elif violation_rows:
            steps.append(f"Fix rows in `{violation_table}` (see table below), then re-run the test.")
        elif violation_note:
            steps.append(violation_note)
        elif compiled_rel and result_line and re.search(r"Got\s+[1-9]", result_line):
            steps.append(
                "dbt reported failures but preview is empty now — re-run test; "
                "if still failing, run compiled SQL below."
            )

    elif "dbt_run_" in task_id:
        model_fail = re.search(r"== \[([^\]]+)\] FAIL", log_text)
        db_err = re.search(r"Database Error\s*\n\s*([^\n]+)", log_text)
        if model_fail:
            root_cause = f"dbt model failed: {model_fail.group(1)}"
            steps.append(f"Re-run: `dbt run --select {model_fail.group(1)}`")
        else:
            root_cause = "dbt run failed"
        if db_err:
            root_cause += f" — {db_err.group(1).strip()}"
            steps.append(f"Warehouse error: {db_err.group(1).strip()}")

    elif "dbt_source_freshness" in task_id:
        stale = re.search(
            r"Source '([^']+)'[\s\S]{0,200}?(exceeded|error|stale)",
            log_text,
            re.IGNORECASE,
        )
        root_cause = (
            f"Source freshness failed: {stale.group(1)} ({stale.group(2)})"
            if stale
            else "Source freshness SLA failed"
        )
        src = re.search(r"source:([\w_]+)", log_text)
        if src:
            steps.append(f"Re-run: `dbt source freshness --select source:{src.group(1)}`")
        steps.append("Confirm the latest Airbyte sync for this pipeline completed successfully.")

    elif "extraction" in task_id:
        job = re.search(r"job[_ ]?id[=: ]+(\d+)", log_text, re.IGNORECASE)
        err = re.search(r"(error|failed)[^\n]{0,120}", log_text, re.IGNORECASE)
        root_cause = f"Airbyte sync failed (job {job.group(1)})" if job else "Airbyte extraction failed"
        if err:
            root_cause += f" — {err.group(0)[:100]}"
        steps.append("Inspect the failed Airbyte job and the first stream-level error in its logs.")

    else:
        root_cause = str(exception)[:200] if exception else "Task failed — see log highlights"
        steps.append("Open the Airflow task log link below.")

    body_lower = f"{exception}\n{log_text}".lower()
    if "password authentication failed" in body_lower:
        steps.insert(0, "Fix DBT_WAREHOUSE_* credentials in scheduler env, then `dbt debug`.")
    if "relation does not exist" in body_lower:
        m = re.search(r'relation "([^"]+)" does not exist', log_text, re.IGNORECASE)
        if m:
            steps.insert(0, f"Missing relation `{m.group(1)}` — run upstream models or check schema.")
        else:
            steps.insert(0, "Missing table — run upstream dbt models.")

    deduped: list[str] = []
    for s in steps:
        if s and s not in deduped:
            deduped.append(s)

    return FailureInsight(
        root_cause=root_cause,
        steps=deduped[:4],
        test_name=test_name,
        test_resource=test_resource,
        test_type=test_type,
        result_line=result_line,
        compiled_sql_path=compiled_rel,
        compiled_sql_snippet=compiled_snippet,
        log_highlights=log_slice or _extract_log_highlights(log_text),
        dbt_summary=dbt_summary,
        violation_rows=violation_rows,
        violation_table=violation_table,
        violation_query_note=violation_note,
        child_column=child_column,
    )


def _build_log_url(context: dict[str, Any]) -> str:
    ti = context["task_instance"]
    base = os.environ.get("AIRFLOW_WEBSERVER_BASE_URL", "http://localhost:8080").rstrip("/")
    run_id = getattr(ti, "run_id", None) or context.get("run_id", "")
    if run_id:
        return (
            f"{base}/dags/{ti.dag_id}/grid"
            f"?dag_run_id={html.escape(run_id, quote=True)}"
            f"&task_id={html.escape(ti.task_id, quote=True)}"
            f"&tab=logs"
        )
    return f"{base}/dags/{ti.dag_id}/grid"


def _facts_table_html(insight: FailureInsight) -> str:
    rows: list[str] = []
    if insight.test_name:
        rows.append(
            f"<tr><td><b>Test</b></td><td><code>{html.escape(insight.test_name)}</code></td></tr>"
        )
    if insight.test_resource:
        rows.append(
            f"<tr><td><b>Config</b></td><td><code>{html.escape(insight.test_resource)}</code></td></tr>"
        )
    if insight.result_line:
        rows.append(f"<tr><td><b>dbt</b></td><td>{html.escape(insight.result_line)}</td></tr>")
    if insight.dbt_summary:
        rows.append(f"<tr><td><b>Run</b></td><td>{html.escape(insight.dbt_summary)}</td></tr>")
    if insight.violation_table:
        rows.append(
            f"<tr><td><b>Table</b></td><td><code>{html.escape(insight.violation_table)}</code></td></tr>"
        )
    if not rows:
        return ""
    return (
        "<table cellpadding='6' style='border-collapse:collapse;margin:12px 0;'>"
        + "".join(rows)
        + "</table>"
    )


def _violations_table_html(rows: list[dict[str, Any]], child_column: str = "") -> str:
    if not rows:
        return ""
    cols = list(rows[0].keys())
    title = "Violating keys (live warehouse query, max 5)"
    if child_column:
        title += f" — column `{child_column}`"
    head = "".join(f"<th style='text-align:left;padding:4px 8px;'>{html.escape(c)}</th>" for c in cols)
    body_rows = []
    for row in rows:
        tds = "".join(
            f"<td style='padding:4px 8px;border-top:1px solid #e5e7eb;'>"
            f"<code>{html.escape(str(row.get(c, '')))}</code></td>"
            for c in cols
        )
        body_rows.append(f"<tr>{tds}</tr>")
    return (
        f"<h3 style='margin-top:16px;color:#b42318;'>{html.escape(title)}</h3>"
        "<table style='border-collapse:collapse;font-size:13px;'>"
        f"<thead><tr>{head}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody></table>"
    )


def elt_task_failure_email(context: dict[str, Any]) -> None:
    recipients = get_alert_recipients()
    if not recipients:
        return

    ti = context["task_instance"]
    dag = context["dag"]
    exception = context.get("exception")
    try_number = getattr(ti, "try_number", None)
    max_tries = getattr(ti, "max_tries", None)

    log_text = _collect_log_text(context)
    insight = _build_insight(ti.task_id, exception, log_text)
    log_url = _build_log_url(context)

    subject = f"[ELT ALERT][FAILED] {dag.dag_id} · {ti.task_id}"
    if insight.test_name:
        short = insight.test_name[:60] + ("..." if len(insight.test_name) > 60 else "")
        subject += f" · {short}"
    if insight.result_line:
        m = re.search(r"Got\s+(\d+)", insight.result_line)
        if m:
            subject += f" · {m.group(0)}"
    subject += f" · try {try_number}/{max_tries}"

    body_html = f"""
    <html><body style="font-family:system-ui,sans-serif;line-height:1.5;color:#111;">
      <h2 style="color:#b42318;">{html.escape(insight.root_cause)}</h2>

      <table cellpadding="6" style="border-collapse:collapse;">
        <tr><td><b>DAG</b></td><td>{html.escape(dag.dag_id)}</td></tr>
        <tr><td><b>Task</b></td><td>{html.escape(ti.task_id)}</td></tr>
        <tr><td><b>Run</b></td><td><code style="font-size:11px;">{html.escape(str(ti.run_id))}</code></td></tr>
      </table>

      {_facts_table_html(insight)}
      {_violations_table_html(insight.violation_rows, insight.child_column)}

      {"<h3>Next steps</h3><ol>" + "".join(f"<li>{html.escape(s)}</li>" for s in insight.steps) + "</ol>" if insight.steps else ""}

      {(
        "<h3>Compiled test SQL (dbt)</h3>"
        f"<p style='font-size:12px;color:#555;'><code>{html.escape(insight.compiled_sql_path)}</code></p>"
        "<pre style='background:#1e293b;color:#e2e8f0;padding:12px;font-size:12px;white-space:pre-wrap;'>"
        + html.escape(insight.compiled_sql_snippet) + "</pre>"
      ) if insight.compiled_sql_snippet else ""}

      {(
        "<h3>dbt log (failure block)</h3>"
        "<pre style='background:#f4f4f5;padding:12px;font-size:12px;white-space:pre-wrap;border:1px solid #e5e7eb;'>"
        + html.escape(insight.log_highlights) + "</pre>"
      ) if insight.log_highlights else ""}

      <p><a href="{html.escape(log_url)}">Full logs in Airflow</a></p>
      <p style="color:#888;font-size:11px;">Evidence from task log + compiled SQL"
      {" + warehouse preview query" if insight.violation_rows else ""}.</p>
    </body></html>
    """

    try:
        from airflow.utils.email import send_email

        send_email(to=recipients, subject=subject, html_content=body_html)
        logger.info("Failure alert sent for %s.%s", dag.dag_id, ti.task_id)
    except Exception as exc:
        logger.exception("Could not send failure email: %s", exc)
