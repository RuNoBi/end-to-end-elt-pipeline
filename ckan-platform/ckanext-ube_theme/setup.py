from setuptools import setup

setup(
    name="ckanext-ube-theme",
    version="0.1.0",
    description="UBE Group Thailand catalog branding and UX",
    packages=["ckanext.ube_theme"],
    namespace_packages=["ckanext"],
    install_requires=[],
    entry_points="""
        [ckan.plugins]
        ube_theme=ckanext.ube_theme.plugin:UbeThemePlugin
    """,
)
