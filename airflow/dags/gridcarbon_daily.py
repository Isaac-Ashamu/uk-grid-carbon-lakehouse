"""Daily pipeline: one DAG run per UTC day, backfillable.

Schedule: 06:00 UTC. By then ESO has published actual intensity for every
half hour of the previous day and Open-Meteo's archive has the day's weather.
The run for data interval ``[D, D+1)`` processes partition ``D``.

Design notes
------------
* Thin DAG, thick library: every task calls a function in ``gridcarbon.pipeline``
  that is unit-tested without Airflow. The DAG only decides *when* and *in what
  order*.
* Extraction and transformation are per dataset so a single flaky source
  retries alone and the others are not re-fetched.
* ``dbt build`` and the Postgres load share the ``gridcarbon_warehouse`` pool
  (1 slot) because DuckDB allows one writer per file. Backfills therefore
  overlap on extraction (``max_active_runs=3``) and serialise on the warehouse.
* The quality gate reads the audit trail for this partition and fails the run
  if any transform failed or the quarantine rate is above the threshold, so a
  bad day never reaches Postgres silently.
* ``outlets`` publish an Airflow Dataset so downstream DAGs (e.g. the API
  smoke test) can be data-aware rather than time-based.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task, task_group

from gridcarbon.sources import DAILY_DATASETS
from gridcarbon_common import MARTS_DATASET, QUARANTINE_RATE_LIMIT_PCT, WAREHOUSE_POOL, default_args


@dag(
    dag_id="gridcarbon_daily",
    description="Bronze -> silver -> dbt marts -> Postgres for one UTC day of grid carbon data.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=True,
    max_active_runs=3,
    max_active_tasks=8,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    tags=["gridcarbon", "daily", "medallion"],
    doc_md=__doc__,
)
def gridcarbon_daily() -> None:
    @task
    def partition_key(data_interval_start: datetime | None = None) -> str:
        assert data_interval_start is not None
        return data_interval_start.strftime("%Y-%m-%d")

    @task_group(group_id="extract")
    def extract_all(day: str) -> list[Any]:
        outs = []
        for _source, dataset in DAILY_DATASETS:

            @task(task_id=f"extract_{dataset}", retries=5)
            def extract_one(day: str, dataset: str = dataset) -> dict[str, Any]:
                from gridcarbon import pipeline
                from gridcarbon.domain import Partition

                records = pipeline.extract(Partition.parse(day), [dataset])
                return {"dataset": dataset, "files": [r.path for r in records]}

            outs.append(extract_one(day))
        return outs

    @task_group(group_id="transform")
    def transform_all(day: str, extracted: list[Any]) -> list[Any]:
        outs = []
        for (_source, dataset), upstream in zip(DAILY_DATASETS, extracted, strict=True):

            @task(task_id=f"transform_{dataset}")
            def transform_one(day: str, _upstream: Any, dataset: str = dataset) -> dict[str, Any]:
                from gridcarbon import pipeline
                from gridcarbon.domain import Partition

                report = pipeline.transform(Partition.parse(day), [dataset])
                return {
                    "dataset": dataset,
                    "rows_out": report.rows_out,
                    "rows_quarantined": report.rows_quarantined,
                }

            outs.append(transform_one(day, upstream))
        return outs

    @task(pool=WAREHOUSE_POOL, outlets=[MARTS_DATASET], retries=1)
    def build_marts(_transformed: list[Any]) -> dict[str, Any]:
        from gridcarbon import pipeline

        out = pipeline.build_marts()
        out.pop("stdout_tail", None)
        return out

    @task
    def quality_gate(day: str, _built: dict[str, Any]) -> dict[str, Any]:
        """Fail the run if this partition's transforms failed or rejected too much."""
        from gridcarbon import warehouse

        frame = warehouse.query(
            "SELECT dataset, transform_status, rows_in, rows_quarantined, quarantine_rate_pct, "
            "national_day_complete FROM marts.data_quality_daily WHERE date_day = ?",
            [day],
        )
        if frame.empty:
            raise RuntimeError(f"no data_quality_daily rows for {day}")
        failed = frame[frame["transform_status"] != "succeeded"]
        if not failed.empty:
            raise RuntimeError(f"transforms not succeeded for {day}: {failed['dataset'].tolist()}")
        hot = frame[frame["quarantine_rate_pct"].fillna(0) > QUARANTINE_RATE_LIMIT_PCT]
        if not hot.empty:
            raise RuntimeError(
                f"quarantine rate above {QUARANTINE_RATE_LIMIT_PCT}% for {day}: "
                + ", ".join(f"{r.dataset}={r.quarantine_rate_pct}%" for r in hot.itertuples())
            )
        return {
            "datasets": len(frame),
            "quarantined": int(frame["rows_quarantined"].fillna(0).sum()),
            "national_day_complete": bool(frame["national_day_complete"].iloc[0]),
        }

    @task(pool=WAREHOUSE_POOL, retries=2)
    def load_postgres(_gate: dict[str, Any]) -> dict[str, int]:
        from gridcarbon import pipeline

        return pipeline.load_serving()

    day = partition_key()
    extracted = extract_all(day)
    transformed = transform_all(day, extracted)
    built = build_marts(transformed)
    gate = quality_gate(day, built)
    load_postgres(gate)


gridcarbon_daily()
