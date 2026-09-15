"""Data-aware DAG: whenever the marts dataset is updated, hit the API's health
and coverage endpoints in-process and fail if the newest day is not served.

This is what "the pipeline is done" means from a consumer's point of view.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task

from gridcarbon_common import MARTS_DATASET, default_args


@dag(
    dag_id="gridcarbon_api_smoke",
    description="Verify the read API serves the freshly built marts.",
    schedule=[MARTS_DATASET],
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={**default_args, "retries": 1},
    dagrun_timeout=timedelta(minutes=10),
    tags=["gridcarbon", "smoke", "dataset-triggered"],
    doc_md=__doc__,
)
def gridcarbon_api_smoke() -> None:
    @task
    def check_api() -> dict[str, Any]:
        from fastapi.testclient import TestClient

        from gridcarbon.api.app import app

        client = TestClient(app)
        health = client.get("/health")
        health.raise_for_status()
        body = health.json()
        if not body.get("last_day"):
            raise RuntimeError(f"API reports no data: {body}")
        latest = client.get("/intensity/national", params={"day": body["last_day"]})
        latest.raise_for_status()
        return {
            "last_day": body["last_day"],
            "rows": len(latest.json()),
            "backend": body["backend"],
        }

    check_api()


gridcarbon_api_smoke()
