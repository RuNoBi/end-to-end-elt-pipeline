FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"

COPY airflow-platform/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"

COPY airflow-platform/requirements-dbt.txt /requirements-dbt.txt
RUN python -m venv /home/airflow/.dbt-venv \
    && /home/airflow/.dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/.dbt-venv/bin/pip install --no-cache-dir -r /requirements-dbt.txt

# Immutable dbt packages (read-only). Not on the ../dbt-warehouse bind mount, so many
# DAGs can run dbt concurrently without racing on dbt_packages/.
USER root
RUN mkdir -p /opt/dbt-vendor-install /opt/dbt-vendor \
    && chown -R airflow:root /opt/dbt-vendor-install /opt/dbt-vendor
COPY --chown=airflow:root dbt-warehouse/packages.yml /opt/dbt-vendor-install/packages.yml
USER airflow
RUN printf '%s\n' \
    'name: dbt_vendor_bootstrap' \
    'version: 1.0.0' \
    'config-version: 2' \
    'profile: de_poc_warehouse' \
    'packages-install-path: /opt/dbt-vendor/dbt_packages' \
    > /opt/dbt-vendor-install/dbt_project.yml \
    && /home/airflow/.dbt-venv/bin/dbt deps --project-dir /opt/dbt-vendor-install \
    && test -f /opt/dbt-vendor/dbt_packages/dbt_utils/dbt_project.yml

ENV PATH="/home/airflow/.dbt-venv/bin:${PATH}"
ENV DBT_PACKAGES_INSTALL_PATH=/opt/dbt-vendor/dbt_packages
