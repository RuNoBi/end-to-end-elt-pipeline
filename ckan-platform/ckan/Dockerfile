# CKAN 2.10 + UBE Group Thailand theme (datastore / datapusher via compose env).
FROM ckan/ckan-base:2.10.8-py3.10

USER root
COPY ckanext-ube_theme /srv/app/ckanext-ube_theme
RUN pip install --no-cache-dir -e /srv/app/ckanext-ube_theme \
    && chown -R ckan:ckan-sys /srv/app/ckanext-ube_theme

USER ckan
