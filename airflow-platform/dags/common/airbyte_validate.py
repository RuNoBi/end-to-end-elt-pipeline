"""
Preflight checks for Airbyte OSS connections before triggering a sync.

Fails fast when streams are still on full_refresh/overwrite (unsafe for large Bronze).
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

_EXPECTED_STREAMS: dict[str, dict[str, Any]] = {
    "customers": {
        "syncMode": "incremental",
        "destinationSyncMode": "append_dedup",
        "cursorField": ["created_at"],
    },
    "orders": {
        "syncMode": "incremental",
        "destinationSyncMode": "append_dedup",
        "cursorField": ["order_date"],
    },
}


def _normalize_cursor(cursor: Any) -> list[str]:
    if cursor is None:
        return []
    if isinstance(cursor, str):
        return [cursor]
    return [str(c) for c in cursor]


def validate_airbyte_connection(
    api_base_url: str,
    connection_id: str,
    *,
    timeout_seconds: int = 60,
) -> dict[str, Any]:
    """
    Verify connection exists and every configured stream uses incremental append_dedup.

    Returns a summary dict (stream names, schedule) for logging / XCom.
    """
    base = api_base_url.rstrip("/")
    response = requests.post(
        f"{base}/connections/get",
        json={"connectionId": connection_id},
        timeout=timeout_seconds,
    )
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        raise AirflowException(
            f"Airbyte connections/get failed for {connection_id}: {exc}"
        ) from exc

    connection = response.json()
    streams = (connection.get("syncCatalog") or {}).get("streams") or []
    if not streams:
        raise AirflowException(
            f"Airbyte connection {connection_id} has no streams in syncCatalog."
        )

    issues: list[str] = []
    configured: list[dict[str, Any]] = []

    for entry in streams:
        stream = entry.get("stream") or {}
        config = entry.get("config") or {}
        if not config.get("selected", True):
            continue

        name = stream.get("name") or "unknown"
        sync_mode = config.get("syncMode")
        dest_mode = config.get("destinationSyncMode")
        cursor = _normalize_cursor(config.get("cursorField"))

        configured.append(
            {
                "name": name,
                "syncMode": sync_mode,
                "destinationSyncMode": dest_mode,
                "cursorField": cursor,
            }
        )

        expected = _EXPECTED_STREAMS.get(name)
        if expected is None:
            logger.warning("Airbyte stream %s is not in the expected catalog template", name)
            continue

        if sync_mode != expected["syncMode"] or dest_mode != expected["destinationSyncMode"]:
            issues.append(
                f"{name}: syncMode={sync_mode!r} destinationSyncMode={dest_mode!r} "
                f"(expected incremental + append_dedup)"
            )
        elif cursor != expected["cursorField"]:
            issues.append(
                f"{name}: cursorField={cursor!r} (expected {expected['cursorField']!r})"
            )

    missing = sorted(set(_EXPECTED_STREAMS) - {s["name"] for s in configured})
    if missing:
        issues.append(f"missing streams: {', '.join(missing)}")

    if issues:
        raise AirflowException(
            "Airbyte connection is not production-safe:\n- "
            + "\n- ".join(issues)
            + "\nFix in Airbyte UI → Connections → source → dwh, then re-run."
        )

    summary = {
        "connectionId": connection_id,
        "name": connection.get("name"),
        "scheduleType": connection.get("scheduleType"),
        "streams": configured,
    }
    logger.info(
        "Airbyte preflight OK | connection=%s | schedule=%s | streams=%s",
        summary["name"],
        summary["scheduleType"],
        [s["name"] for s in configured],
    )
    return summary
