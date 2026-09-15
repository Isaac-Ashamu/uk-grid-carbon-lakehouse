# ADR 0002: UTC everywhere, and why a dbt test caught a timezone bug

**Status:** accepted · **Date:** 2026-09-15

## Context

The Carbon Intensity API reports half-hour *settlement periods* in UTC
(`2026-09-01T00:30Z`). Open-Meteo is requested with `timezone=UTC`. The pipeline
partitions by UTC calendar day. Great Britain, however, is on BST (UTC+1) for
half the year, and the machine the pipeline first ran on was in Europe/London.

## What happened

The first `dbt build` over three fully loaded days failed
`assert_no_missing_settlement_periods`: 1 September had 46 periods and a
phantom 4 September had 2. Silver was correct (48 rows per day). DuckDB's
session `TimeZone` defaulted to the host's local zone, so
`period_start::date` on a `TIMESTAMP WITH TIME ZONE` moved the day boundary
by one hour.

## Decision

* Every DuckDB connection the pipeline opens runs `SET TimeZone = 'UTC'`
  (`warehouse.connect`) and the dbt profile sets `settings: { TimeZone: UTC }`.
* Silver timestamps are stored as timezone-aware UTC; the partition column is
  the UTC day.
* `settlement_period` (1 to 48) is derived from UTC hour and minute, so it is
  stable across the BST clock changes. Local-time presentation is a concern
  for the consumer, not the warehouse.

## Consequences

The singular test stays in the project as a regression guard. It is exactly
the class of bug that silently corrupts daily aggregates if nothing asserts
that a day has 48 periods.
