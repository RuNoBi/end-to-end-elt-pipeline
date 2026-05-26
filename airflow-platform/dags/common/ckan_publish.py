"""
Publish Gold warehouse tables to CKAN Datastore (open data catalog / datamart UI).

Pattern: copy mart data from analytics Postgres → CKAN Datastore API (idempotent upsert).
Requires CKAN_API_TOKEN and organization from bootstrap-ckan.sh.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg2
import psycopg2.extras
import requests
from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)

# Gold tables exposed as CKAN datasets (aggregated mart first — safe at scale).
_CATALOG_OWNER = "UBE Group Thailand — Data Platform"
_CATALOG_LICENSE = "cc-by-4.0"

GOLD_PUBLICATIONS: list[dict[str, str]] = [
    {
        "package_name": "sales-performance-mart",
        "package_title": "Sales Performance Mart",
        "resource_name": "mart_sales_performance",
        "schema": "gold",
        "table": "mart_sales_performance",
        "description": (
            "Monthly sales KPIs by customer, segment, and order status "
            "(dbt Gold `mart_sales_performance`)."
        ),
        "primary_key": "customer_id,order_month,order_status,order_value_segment",
        "frequency": "daily",
    },
    {
        "package_name": "customer-dimension",
        "package_title": "Customer Dimension",
        "resource_name": "dim_customer",
        "schema": "gold",
        "table": "dim_customer",
        "description": "Conformed customer dimension — current attributes (dbt Gold `dim_customer`).",
        "primary_key": "customer_id",
        "max_rows": "50000",
        "frequency": "daily",
    },
]

PG_TO_CKAN_TYPE: dict[str, str] = {
    "integer": "int",
    "bigint": "int",
    "smallint": "int",
    "numeric": "numeric",
    "double precision": "float",
    "real": "float",
    "boolean": "bool",
    "date": "date",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamp",
    "text": "text",
    "character varying": "text",
}


def _ckan_url() -> str:
    return os.environ.get("CKAN_URL", "http://ckan:5000").rstrip("/")


def _ckan_token() -> str:
    token = os.environ.get("CKAN_API_TOKEN") or Variable.get("ckan_api_token", default_var="")
    if not token:
        raise AirflowException(
            "CKAN_API_TOKEN not set. Run ckan-platform/scripts/bootstrap-ckan.sh "
            "and add AIRFLOW_VAR_CKAN_API_TOKEN to airflow-platform/.env"
        )
    return token


def _ckan_org() -> str:
    return os.environ.get("CKAN_ORGANIZATION") or Variable.get(
        "ckan_organization", default_var="ube-group-thailand"
    )


def _max_rows() -> int:
    raw = os.environ.get("CKAN_PUBLISH_MAX_ROWS") or Variable.get(
        "ckan_publish_max_rows", default_var="100000"
    )
    return int(raw)


def _upsert_batch_size() -> int:
    raw = os.environ.get("CKAN_UPSERT_BATCH_SIZE") or Variable.get(
        "ckan_upsert_batch_size", default_var="2000"
    )
    return max(100, int(raw))


def _warehouse_conn():
    return psycopg2.connect(
        host=os.environ["DBT_WAREHOUSE_HOST"],
        port=int(os.environ.get("DBT_WAREHOUSE_PORT", "5432")),
        user=os.environ["DBT_WAREHOUSE_USER"],
        password=os.environ["DBT_WAREHOUSE_PASSWORD"],
        dbname=os.environ["DBT_WAREHOUSE_DB"],
    )


def _ckan_error_type(error: Any) -> str | None:
    if isinstance(error, dict):
        return error.get("__type")
    return None


def _is_auth_error(error: Any) -> bool:
    return isinstance(error, dict) and error.get("__type") == "Authorization Error"


_AUTH_FAILED = object()


def _ckan_action(
    action: str,
    payload: dict[str, Any],
    *,
    allow_not_found: bool = False,
    allow_auth_failure: bool = False,
) -> Any:
    url = f"{_ckan_url()}/api/3/action/{action}"
    response = requests.post(
        url,
        json=payload,
        headers={"Authorization": _ckan_token()},
        timeout=300,
    )
    try:
        body = response.json()
    except ValueError as exc:
        raise AirflowException(
            f"CKAN {action} returned non-JSON (HTTP {response.status_code}): "
            f"{response.text[:500]}"
        ) from exc

    if body.get("success"):
        return body.get("result")

    error = body.get("error")
    if allow_not_found and _ckan_error_type(error) == "Not Found Error":
        return None

    if allow_auth_failure and _is_auth_error(error):
        return _AUTH_FAILED

    raise AirflowException(f"CKAN {action} failed: {error}")


def _ckan_preflight() -> None:
    """Fail fast with actionable errors before copying warehouse rows."""
    status = _ckan_action("status_show", {})
    logger.info("CKAN status_show: %s", status)

    org = _ckan_org()
    org_result = _ckan_action("organization_show", {"id": org}, allow_not_found=True)
    if not org_result:
        raise AirflowException(
            f"CKAN organization '{org}' not found. Run ckan-platform/scripts/bootstrap-ckan.sh "
            f"or create the org in the CKAN UI."
        )

    # Prove token can write (stale tokens after ckan-db recreate cause package_patch 403).
    probe = _ckan_action(
        "package_patch",
        {"id": "sales-performance-mart", "notes": status.get("site_title", "probe")},
        allow_auth_failure=True,
    )
    if probe is _AUTH_FAILED:
        raise AirflowException(
            "CKAN API token cannot edit datasets. Run: cd ckan-platform && ./scripts/bootstrap-ckan.sh "
            "then copy CKAN_API_TOKEN into airflow-platform/.env and restart airflow-scheduler."
        )


def _json_serialize(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    return value


def _infer_fields_from_pg(schema: str, table: str) -> list[dict[str, str]]:
    with _warehouse_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select column_name, data_type
                from information_schema.columns
                where table_schema = %s and table_name = %s
                order by ordinal_position
                """,
                (schema, table),
            )
            return [
                {"id": row[0], "type": PG_TO_CKAN_TYPE.get(row[1], "text")}
                for row in cur.fetchall()
            ]


def _parse_primary_key(pub: dict[str, str], columns: list[str]) -> str | list[str]:
    raw = pub.get("primary_key") or (columns[0] if columns else "id")
    if "," in raw:
        return [part.strip() for part in raw.split(",") if part.strip()]
    return raw


def _iter_table_batches(
    schema: str,
    table: str,
    limit: int,
    batch_size: int,
) -> Any:
    """Stream warehouse rows in fixed-size batches (avoids OOM on large marts)."""
    query = f"SELECT * FROM {schema}.{table} LIMIT %s"
    with _warehouse_conn() as conn:
        cursor_name = f"ckan_{schema}_{table}"[:60]
        with conn.cursor(
            name=cursor_name,
            cursor_factory=psycopg2.extras.RealDictCursor,
        ) as cur:
            cur.itersize = batch_size
            cur.execute(query, (limit,))
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                yield [
                    {k: _json_serialize(v) for k, v in dict(row).items()}
                    for row in rows
                ]


def _package_notes(pub: dict[str, str]) -> str:
    freq = pub.get("frequency", "daily")
    return (
        f"{pub['description']}\n\n"
        f"**Owner:** {_CATALOG_OWNER}  \n"
        f"**Update frequency:** {freq}  \n"
        f"**Source:** PostgreSQL warehouse (`{pub['schema']}.{pub['table']}`)  \n"
        f"**Usage:** Open this dataset → resource → **Data Explorer** to preview rows."
    )


def _ensure_package(pub: dict[str, str]) -> dict[str, Any]:
    name = pub["package_name"]
    title = pub["package_title"]
    notes = _package_notes(pub)
    existing = _ckan_action("package_show", {"id": name}, allow_not_found=True)
    if existing:
        org_name = (existing.get("organization") or {}).get("name")
        patch_payload: dict[str, Any] = {
            "id": name,
            "title": title,
            "notes": notes,
            "author": _CATALOG_OWNER,
            "license_id": _CATALOG_LICENSE,
            "version": date.today().isoformat(),
        }
        if org_name != _ckan_org():
            patch_payload["owner_org"] = _ckan_org()
        logger.info("CKAN package exists: %s — updating metadata", name)
        patched = _ckan_action("package_patch", patch_payload, allow_auth_failure=True)
        if patched is _AUTH_FAILED:
            raise AirflowException(
                "CKAN API token cannot edit datasets (Authorization Error). "
                "Run ckan-platform/scripts/bootstrap-ckan.sh, update CKAN_API_TOKEN in "
                "airflow-platform/.env, then: docker compose up -d airflow-scheduler"
            )
        return patched if patched is not None else existing
    logger.info("Creating CKAN package: %s (org=%s)", name, _ckan_org())
    return _ckan_action(
        "package_create",
        {
            "name": name,
            "title": title,
            "owner_org": _ckan_org(),
            "notes": notes,
            "author": _CATALOG_OWNER,
            "license_id": _CATALOG_LICENSE,
            "version": date.today().isoformat(),
            "tags": [
                {"name": "gold"},
                {"name": "elt"},
                {"name": "dbt"},
                {"name": "ube-group-thailand"},
            ],
            "extras": [
                {"key": "update_frequency", "value": pub.get("frequency", "daily")},
                {"key": "language", "value": "en"},
            ],
        },
    )


def _ensure_resource(package_name: str, resource_name: str, description: str) -> str:
    pkg = _ckan_action("package_show", {"id": package_name})
    for res in pkg.get("resources", []):
        if res.get("name") == resource_name:
            return res["id"]

    created = _ckan_action(
        "resource_create",
        {
            "package_id": package_name,
            "name": resource_name,
            "description": description,
            "format": "CSV",
            "mimetype": "text/csv",
            "url_type": "datastore",
        },
    )
    return created["id"]


def _ensure_datatables_view(resource_id: str) -> None:
    views = _ckan_action("resource_view_list", {"id": resource_id}) or []
    if any(v.get("view_type") == "datatables_view" for v in views):
        return
    _ckan_action(
        "resource_view_create",
        {
            "resource_id": resource_id,
            "view_type": "datatables_view",
            "title": "Data Explorer",
        },
    )
    logger.info("CKAN Data Explorer view created for resource %s", resource_id)


def _publish_table(pub: dict[str, str]) -> dict[str, Any]:
    schema = pub["schema"]
    table = pub["table"]
    limit = int(pub.get("max_rows") or _max_rows())
    batch_size = _upsert_batch_size()

    logger.info(
        "Publishing %s.%s to CKAN package=%s (limit=%s, batch=%s)",
        schema,
        table,
        pub["package_name"],
        limit,
        batch_size,
    )

    _ensure_package(pub)
    resource_id = _ensure_resource(
        pub["package_name"], pub["resource_name"], pub["description"]
    )

    fields = _infer_fields_from_pg(schema, table)
    if not fields:
        logger.warning("No columns for %s.%s — skipping datastore upsert", schema, table)
        return {"resource_id": resource_id, "rows": 0}

    column_names = [field["id"] for field in fields]
    primary_key = _parse_primary_key(pub, column_names)

    _ckan_action(
        "datastore_create",
        {
            "resource_id": resource_id,
            "force": True,
            "primary_key": primary_key,
            "fields": fields,
        },
    )

    total = 0
    for batch in _iter_table_batches(schema, table, limit, batch_size):
        _ckan_action(
            "datastore_upsert",
            {
                "resource_id": resource_id,
                "method": "upsert",
                "records": batch,
            },
        )
        total += len(batch)
        if total % (batch_size * 5) == 0:
            logger.info("CKAN upsert progress: %s rows → %s", total, pub["package_name"])

    if total == 0:
        logger.warning("No rows in %s.%s — datastore schema created only", schema, table)
    else:
        _ensure_datatables_view(resource_id)

    logger.info(
        "Published %s rows to CKAN resource %s (%s)",
        total,
        resource_id,
        pub["package_name"],
    )
    return {"resource_id": resource_id, "rows": total, "package": pub["package_name"]}


def publish_gold_to_ckan(**context: Any) -> list[dict[str, Any]]:
    """Airflow callable: refresh all configured Gold datasets in CKAN."""
    _ckan_preflight()
    results: list[dict[str, Any]] = []
    for pub in GOLD_PUBLICATIONS:
        results.append(_publish_table(pub))

    logger.info("CKAN publication complete: %s", results)
    return results
