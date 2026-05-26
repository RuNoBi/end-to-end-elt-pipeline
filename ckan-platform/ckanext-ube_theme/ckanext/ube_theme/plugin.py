"""UBE Group Thailand — site branding (CSS, logo, homepage)."""

from __future__ import annotations

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit


class UbeThemePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer, inherit=True)

    def update_config(self, config: dict) -> None:
        toolkit.add_template_directory(config, "templates")
        toolkit.add_public_directory(config, "public")
