"""Catalog domain / layer metadata for grouped CKAN UI."""

from __future__ import annotations

from typing import Any

CATALOG_DOMAINS: dict[str, dict[str, Any]] = {
    "retail-sales": {
        "id": "retail-sales",
        "title": "Retail Sales",
        "description": (
            "Customer and order data — dimensions, facts, and the sales performance mart."
        ),
        "icon": "fa-shopping-cart",
        "sort_order": 1,
    },
    "sap-chemicals": {
        "id": "sap-chemicals",
        "title": "SAP Chemical Sales",
        "description": (
            "SAP-style chemical sales: materials, business partners, and order lines."
        ),
        "icon": "fa-flask",
        "sort_order": 2,
    },
    "api-reference": {
        "id": "api-reference",
        "title": "API Reference Data",
        "description": (
            "Master data ingested via HTTP API (Dynamics-style country reference)."
        ),
        "icon": "fa-globe",
        "sort_order": 3,
    },
}

CATALOG_LAYERS: dict[str, dict[str, Any]] = {
    "mart": {
        "id": "mart",
        "title": "Analytics & KPIs",
        "description": "Aggregated tables for dashboards and reporting.",
        "sort_order": 1,
    },
    "dimension": {
        "id": "dimension",
        "title": "Dimensions & master data",
        "description": "Conformed attributes for joins and filters.",
        "sort_order": 2,
    },
    "fact": {
        "id": "fact",
        "title": "Facts & transactions",
        "description": "Line-level detail (catalog may show a capped sample).",
        "sort_order": 3,
    },
}


def _extra_map(package: dict[str, Any]) -> dict[str, str]:
    return {e.get("key"): e.get("value") for e in package.get("extras") or [] if e.get("key")}


def infer_catalog_domain(package: dict[str, Any]) -> str:
    extras = _extra_map(package)
    domain = (extras.get("catalog_domain") or "").strip()
    if domain:
        return domain
    name = package.get("name") or ""
    if name.startswith("sap-"):
        return "sap-chemicals"
    if name.startswith("country-") or "api" in name:
        return "api-reference"
    return "retail-sales"


def infer_catalog_layer(package: dict[str, Any]) -> str:
    extras = _extra_map(package)
    layer = (extras.get("catalog_layer") or "").strip()
    if layer:
        return layer
    name = package.get("name") or ""
    if "mart" in name or "performance" in name:
        return "mart"
    if "dimension" in name or "dim_" in name or "materials" in name or "customers" in name:
        return "dimension"
    if "fact" in name or "fct_" in name or "lines" in name or "orders" in name:
        return "fact"
    return "mart"


def catalog_sort_key(package: dict[str, Any]) -> tuple[int, str]:
    extras = _extra_map(package)
    try:
        order = int(extras.get("catalog_order") or 999)
    except ValueError:
        order = 999
    return (order, package.get("title") or package.get("name") or "")
