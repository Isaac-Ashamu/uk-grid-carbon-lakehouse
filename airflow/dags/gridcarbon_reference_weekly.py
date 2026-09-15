"""Weekly refresh of slowly changing reference data (ESO emission factors).

Runs Monday 05:00 UTC, before the daily run. Snapshots the factors under
today's partition, publishes them to silver, and rebuilds only the models
that depend on them.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task

from gridcarbon_common import WAREHOUSE_POOL, default_args


@dag(
    dag_id="gridcarbon_reference_weekly",
    description="Snapshot ESO emission factors and rebuild the fuel dimension.",
    schedule="0 5 * * 1",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=30),
    tags=["gridcarbon", "weekly", "reference"],
    doc_md=__doc__,
)
def gridcarbon_reference_weekly() -> None:
    @task(retries=5)
    def snapshot_factors(logical_date: datetime | None = None) -> dict[str, Any]:
        from gridcarbon import pipeline
        from gridcarbon.audit import new_run_id
        from gridcarbon.domain import Partition

        assert logical_date is not None
        run_id = new_run_id()
        records = pipeline.extract_reference(run_id)
        today = Partition(
            records[0].partition and datetime.fromisoformat(records[0].partition).date()
        )
        report = pipeline.transform(today, ["carbon_factors"], run_id, include_reference=True)
        return {"partition": report.partition, "rows_out": report.rows_out}

    @task(pool=WAREHOUSE_POOL)
    def rebuild_fuel_models(_snap: dict[str, Any]) -> dict[str, Any]:
        from gridcarbon import pipeline

        out = pipeline.build_marts(select="stg_carbon_factors+")
        out.pop("stdout_tail", None)
        return out

    rebuild_fuel_models(snapshot_factors())


gridcarbon_reference_weekly()
