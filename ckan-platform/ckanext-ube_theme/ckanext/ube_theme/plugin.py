"""UBE Group Thailand — site branding (CSS, logo, homepage)."""

from __future__ import annotations

import os

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckanext.ube_theme.helpers import (
    ube_catalog_domains,
    ube_catalog_sections,
    ube_catalog_stats,
    ube_company_name,
    ube_data_explorer_url,
    ube_featured_datasets,
    ube_org_id,
    ube_package_catalog_meta,
    ube_primary_datastore_resource,
)


class UbeThemePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)

    def update_config(self, config: dict) -> None:
        toolkit.add_template_directory(config, "templates")
        toolkit.add_public_directory(config, "public")

        title = os.environ.get("CKAN__SITE__TITLE") or os.environ.get("CKAN_SITE_TITLE")
        if title:
            config["ckan.site_title"] = title.strip().strip('"')
        description = os.environ.get("CKAN__SITE__DESCRIPTION") or os.environ.get(
            "CKAN_SITE_DESCRIPTION"
        )
        if description:
            config["ckan.site_description"] = description.strip().strip('"')

        config["ckan.site_logo"] = "/images/ube-logo.png"
        config["ckan.site_about"] = (
            "UBE Group Thailand open data catalog — Gold datamarts from the enterprise "
            "ELT platform (Airbyte, dbt, Airflow)."
        )
        if not config.get("ckan.ube_org_id"):
            config["ckan.ube_org_id"] = (
                os.environ.get("CKAN__UBE_ORG_ID")
                or os.environ.get("CKAN_ORGANIZATION")
                or "ube-group-thailand"
            ).strip()
        config["ckan.ube_company_name"] = (
            os.environ.get("CKAN_ORGANIZATION_TITLE")
            or os.environ.get("CKAN__ORGANIZATION_TITLE")
            or "UBE Group Thailand"
        ).strip().strip('"')

    def get_helpers(self) -> dict:
        return {
            "ube_org_id": ube_org_id,
            "ube_company_name": ube_company_name,
            "ube_catalog_stats": ube_catalog_stats,
            "ube_catalog_domains": ube_catalog_domains,
            "ube_catalog_sections": ube_catalog_sections,
            "ube_package_catalog_meta": ube_package_catalog_meta,
            "ube_featured_datasets": ube_featured_datasets,
            "ube_data_explorer_url": ube_data_explorer_url,
            "ube_primary_datastore_resource": ube_primary_datastore_resource,
        }
