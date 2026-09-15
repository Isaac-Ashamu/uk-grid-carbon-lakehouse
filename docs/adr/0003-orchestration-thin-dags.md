# ADR 0003: Airflow with thin DAGs over a testable library

**Status:** accepted · **Date:** 2026-09-15

## Context

The pipeline needs scheduling, retries, backfills and observability. It also
needs to be testable without a scheduler running.

## Decision

* All logic lives in `gridcarbon.pipeline` as plain functions taking a
  `Partition`. The CLI, the tests and the DAGs call the same functions.
* `gridcarbon_daily` runs at 06:00 UTC with `catchup=True`; the partition is
  `data_interval_start`, so a run on day D+1 processes day D and a backfill is
  `airflow dags backfill` (or `gridcarbon backfill`).
* Extraction and transformation are one task per dataset so one flaky source
  retries alone. `dbt build` and the Postgres load share a one-slot pool.
* A `quality_gate` task reads `marts.data_quality_daily` for the partition and
  fails the run before anything reaches Postgres if a transform failed or the
  quarantine rate exceeds 5%.
* `build_marts` publishes an Airflow Dataset; `gridcarbon_api_smoke` is
  dataset-triggered and checks the API actually serves the new day.
* Airflow is installed in its own environment (pinned constraints) so its
  dependency tree never fights with dbt's.

## Alternatives considered

Dagster's asset model fits a medallion lakehouse well and has first-class
partitions, but Airflow remains the most widely deployed orchestrator, and the
thin-DAG design means moving to Dagster would touch only the `airflow/` folder.
