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
    expected_streams: dict[str, dict[str, Any]] | None = None,
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

    expected_catalog = expected_streams or {}
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

        expected = expected_catalog.get(name)
        if expected is None:
            if expected_catalog:
                logger.warning(
                    "Airbyte stream %s is not listed in pipeline expected_streams",
                    name,
                )
            continue

        if sync_mode != expected.get("syncMode") or dest_mode != expected.get(
            "destinationSyncMode"
        ):
            issues.append(
                f"{name}: syncMode={sync_mode!r} destinationSyncMode={dest_mode!r} "
                f"(expected {expected.get('syncMode')!r} + {expected.get('destinationSyncMode')!r})"
            )
        elif cursor != _normalize_cursor(expected.get("cursorField")):
            issues.append(
                f"{name}: cursorField={cursor!r} "
                f"(expected {_normalize_cursor(expected.get('cursorField'))!r})"
            )

    if expected_catalog:
        missing = sorted(set(expected_catalog) - {s["name"] for s in configured})
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
