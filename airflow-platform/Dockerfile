FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"

COPY requirements-dbt.txt /requirements-dbt.txt
RUN python -m venv /home/airflow/.dbt-venv \
    && /home/airflow/.dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/.dbt-venv/bin/pip install --no-cache-dir -r /requirements-dbt.txt

ENV PATH="/home/airflow/.dbt-venv/bin:${PATH}"
