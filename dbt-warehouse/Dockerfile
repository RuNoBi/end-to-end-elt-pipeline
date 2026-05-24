FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/usr/app

WORKDIR /usr/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Pin dbt Core + Postgres adapter for reproducible builds
RUN pip install --no-cache-dir \
    dbt-core==1.9.0 \
    dbt-postgres==1.9.0

ENTRYPOINT ["dbt"]
CMD ["--help"]
