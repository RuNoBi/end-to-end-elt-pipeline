"""Template helpers for UBE catalog homepage and navigation."""

from __future__ import annotations

import logging
from typing import Any

import ckan.plugins.toolkit as toolkit

from ckanext.ube_theme.catalog import (
    CATALOG_DOMAINS,
    CATALOG_LAYERS,
    catalog_sort_key,
    infer_catalog_domain,
    infer_catalog_layer,
)

logger = logging.getLogger(__name__)

_DEFAULT_ORG = "ube-group-thailand"


def ube_org_id() -> str:
    return (toolkit.config.get("ckan.ube_org_id") or _DEFAULT_ORG).strip()


def ube_company_name() -> str:
    return toolkit.config.get("ckan.ube_company_name") or "UBE Group Thailand"


def _org_packages(rows: int = 100) -> list[dict[str, Any]]:
    org = ube_org_id()
    try:
        result = toolkit.get_action("package_search")(
            {},
            {"rows": rows, "fq": f"organization:{org}", "sort": "title asc"},
        )
        return list(result.get("results") or [])
    except Exception:
        logger.warning("_org_packages failed", exc_info=True)
        return []


def ube_catalog_stats() -> dict[str, int]:
    """Dataset and resource counts for the UBE organization."""
    org = ube_org_id()
    try:
        packages = _org_packages(100)
        resources = sum(len(p.get("resources") or []) for p in packages)
        domains = len(ube_catalog_domains())
        return {
            "datasets": len(packages),
            "resources": resources,
            "organizations": 1,
            "domains": domains,
        }
    except Exception:
        logger.warning("ube_catalog_stats failed", exc_info=True)
        return {"datasets": 0, "resources": 0, "organizations": 1, "domains": 2}


def ube_catalog_domains() -> list[dict[str, Any]]:
    """Domain cards (retail vs SAP) with dataset counts."""
    packages = _org_packages(100)
    counts: dict[str, int] = {}
    for pkg in packages:
        domain_id = infer_catalog_domain(pkg)
        counts[domain_id] = counts.get(domain_id, 0) + 1

    domains: list[dict[str, Any]] = []
    for domain_id, meta in sorted(
        CATALOG_DOMAINS.items(), key=lambda x: x[1].get("sort_order", 99)
    ):
        row = dict(meta)
        row["dataset_count"] = counts.get(domain_id, 0)
        domains.append(row)
    return domains


def ube_catalog_sections(
    domain_filter: str | None = None,
) -> list[dict[str, Any]]:
    """
    Group datasets by domain → layer for catalog browse UI.
    domain_filter: CKAN group id (retail-sales | sap-chemicals) or None for all.
    """
    packages = _org_packages(100)
    if domain_filter:
        packages = [p for p in packages if infer_catalog_domain(p) == domain_filter]

    by_domain: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for pkg in packages:
        domain_id = infer_catalog_domain(pkg)
        layer_id = infer_catalog_layer(pkg)
        by_domain.setdefault(domain_id, {}).setdefault(layer_id, []).append(pkg)

    sections: list[dict[str, Any]] = []
    domain_ids = [domain_filter] if domain_filter else sorted(
        CATALOG_DOMAINS.keys(), key=lambda d: CATALOG_DOMAINS[d].get("sort_order", 99)
    )

    for domain_id in domain_ids:
        meta = CATALOG_DOMAINS.get(domain_id)
        if not meta:
            continue
        layer_map = by_domain.get(domain_id) or {}
        layers: list[dict[str, Any]] = []
        for layer_id, layer_meta in sorted(
            CATALOG_LAYERS.items(), key=lambda x: x[1].get("sort_order", 99)
        ):
            pkgs = layer_map.get(layer_id) or []
            if not pkgs:
                continue
            pkgs.sort(key=catalog_sort_key)
            layers.append(
                {
                    **layer_meta,
                    "packages": pkgs,
                    "count": len(pkgs),
                }
            )
        if not layers:
            continue
        sections.append(
            {
                **meta,
                "layers": layers,
                "dataset_count": sum(layer["count"] for layer in layers),
            }
        )
    return sections


def ube_package_catalog_meta(package: dict[str, Any]) -> dict[str, str]:
    domain_id = infer_catalog_domain(package)
    layer_id = infer_catalog_layer(package)
    domain = CATALOG_DOMAINS.get(domain_id, {})
    layer = CATALOG_LAYERS.get(layer_id, {})
    return {
        "domain_id": domain_id,
        "domain_title": domain.get("title", domain_id),
        "layer_id": layer_id,
        "layer_title": layer.get("title", layer_id),
    }


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
    """One featured dataset per domain (latest mart)."""
    featured: list[dict[str, Any]] = []
    for domain in ube_catalog_domains():
        domain_id = domain["id"]
        try:
            result = toolkit.get_action("package_search")(
                {},
                {
                    "rows": 1,
                    "fq": f"organization:{ube_org_id()} groups:{domain_id}",
                    "sort": "metadata_modified desc",
                },
            )
            hits = result.get("results") or []
            if hits:
                featured.append(hits[0])
        except Exception:
            logger.warning("ube_featured_datasets domain %s", domain_id, exc_info=True)
    if len(featured) < limit:
        try:
            result = toolkit.get_action("package_search")(
                {},
                {
                    "rows": limit,
                    "fq": f"organization:{ube_org_id()}",
                    "sort": "metadata_modified desc",
                },
            )
            for pkg in result.get("results") or []:
                if pkg not in featured:
                    featured.append(pkg)
                if len(featured) >= limit:
                    break
        except Exception:
            pass
    return featured[:limit]
