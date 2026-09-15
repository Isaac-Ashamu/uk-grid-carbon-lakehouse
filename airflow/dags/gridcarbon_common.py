"""Shared constants for the gridcarbon DAGs. Deliberately defines no DAG."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.datasets import Dataset

MARTS_DATASET = Dataset("gridcarbon://warehouse/marts")

# DuckDB allows a single writer per file: dbt build and the Postgres load
# serialise through this pool. Create it with `airflow pools set gridcarbon_warehouse 1 ...`
# (docker-compose does this on init).
WAREHOUSE_POOL = "gridcarbon_warehouse"

QUARANTINE_RATE_LIMIT_PCT = 5.0

default_args: dict[str, Any] = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "depends_on_past": False,
}
