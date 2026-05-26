"""Template helpers for UBE catalog homepage and navigation."""

from __future__ import annotations

import logging
from typing import Any

import ckan.plugins.toolkit as toolkit

logger = logging.getLogger(__name__)

_DEFAULT_ORG = "ube-group-thailand"


def ube_org_id() -> str:
    return (toolkit.config.get("ckan.ube_org_id") or _DEFAULT_ORG).strip()


def ube_company_name() -> str:
    return toolkit.config.get("ckan.ube_company_name") or "UBE Group Thailand"


def ube_catalog_stats() -> dict[str, int]:
    """Dataset and resource counts for the UBE organization."""
    org = ube_org_id()
    try:
        search = toolkit.get_action("package_search")(
            {},
            {"rows": 0, "fq": f"organization:{org}"},
        )
        dataset_count = int(search.get("count") or 0)
        resources = 0
        if dataset_count:
            detail = toolkit.get_action("package_search")(
                {},
                {"rows": min(dataset_count, 100), "fq": f"organization:{org}"},
            )
            for package in detail.get("results") or []:
                resources += len(package.get("resources") or [])
        return {
            "datasets": dataset_count,
            "resources": resources,
            "organizations": 1,
        }
    except Exception:
        logger.warning("ube_catalog_stats failed", exc_info=True)
        return {"datasets": 0, "resources": 0, "organizations": 1}


def ube_primary_datastore_resource(package: dict[str, Any]) -> dict[str, Any] | None:
    for resource in package.get("resources") or []:
        if resource.get("url_type") == "datastore":
            return resource
    resources = package.get("resources") or []
    return resources[0] if resources else None


def ube_data_explorer_url(package_name: str, resource_id: str | None = None) -> str | None:
    """Direct link to Data Explorer (datatables_view) — one click from catalog UI."""
    try:
        package = toolkit.get_action("package_show")({}, {"id": package_name})
        package_type = package.get("type") or "dataset"
        resource = None
        if resource_id:
            for item in package.get("resources") or []:
                if item["id"] == resource_id:
                    resource = item
                    break
        else:
            resource = ube_primary_datastore_resource(package)
        if not resource:
            return None

        views = toolkit.get_action("resource_view_list")({}, {"id": resource["id"]}) or []
        view_id = None
        for view in views:
            if view.get("view_type") == "datatables_view":
                view_id = view["id"]
                break
        if not view_id and views:
            view_id = views[0]["id"]

        if view_id:
            return toolkit.url_for(
                f"{package_type}_resource.view",
                id=package_name,
                resource_id=resource["id"],
                view_id=view_id,
            )
        return toolkit.url_for(
            f"{package_type}_resource.read",
            id=package_name,
            resource_id=resource["id"],
        )
    except Exception:
        logger.warning("ube_data_explorer_url failed for %s", package_name, exc_info=True)
        return None


def ube_featured_datasets(limit: int = 6) -> list[dict[str, Any]]:
    org = ube_org_id()
    try:
        result = toolkit.get_action("package_search")(
            {},
            {
                "rows": limit,
                "fq": f"organization:{org}",
                "sort": "metadata_modified desc",
            },
        )
        return list(result.get("results") or [])
    except Exception:
        logger.warning("ube_featured_datasets failed", exc_info=True)
        return []
