# Airflow image with the gridcarbon package installed, so DAG tasks import it
# directly (thin DAG, thick library) instead of shelling out to a CLI.
FROM apache/airflow:2.10.5-python3.12
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
USER airflow
COPY --chown=airflow:root pyproject.toml README.md /opt/gridcarbon/
COPY --chown=airflow:root src /opt/gridcarbon/src
COPY --chown=airflow:root dbt /opt/gridcarbon/dbt
RUN pip install --no-cache-dir /opt/gridcarbon
ENV GRIDCARBON_DATA_ROOT=/opt/gridcarbon/data \
    GRIDCARBON_DBT_PROJECT_DIR=/opt/gridcarbon/dbt \
    GRIDCARBON_LOG_JSON=1
