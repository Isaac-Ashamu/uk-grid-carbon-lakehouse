# ADR 0001: Medallion layers on local Parquet with DuckDB as the warehouse

**Status:** accepted · **Date:** 2026-09-15

## Context

The pipeline ingests two public APIs (a few hundred KB per day) and has to be
runnable by one person on a laptop, in CI, and in a container, with no cloud
account. It also has to demonstrate the patterns a production lakehouse uses:
immutable raw storage, validated typed tables, SQL-defined marts, idempotent
partition rewrites and an audit trail.

## Decision

* **Bronze** is raw JSON, one file per HTTP response, in Hive-style
  `source=/dataset=/date=` directories with a manifest sidecar (URL, SHA-256,
  bytes, timing, run id). Append-only; byte-identical responses are not stored twice.
* **Silver** is Parquet (zstd), one directory per UTC day per table.
  Re-processing a day replaces exactly that directory. Contracts are pandera
  schemas; rejects go to a `quarantine` table with the reason and the record.
* **Warehouse** is a single DuckDB file whose `silver.*` relations are views
  over the Parquet, so there is one copy of the data and refreshing after a
  backfill costs nothing.
* **Gold** is dbt (`dbt-duckdb`): staging views dedupe by latest fetch,
  intermediate views pivot and interpolate, marts are tables with schema and
  singular tests.
* **Serving** copies the marts into Postgres (COPY + upsert / atomic swap)
  because that is what dashboards and applications actually connect to.

## Consequences

* The same code runs against `data/` locally, a volume in Docker, and a
  temporary directory in tests. Swapping the lake for S3 means changing the
  writer's path handling, not the pipeline.
* DuckDB has a single writer per file, so `dbt build` and the Postgres load are
  serialised through an Airflow pool. Extraction parallelises freely.
* Silver is the contract boundary: anything downstream can trust types,
  ranges and keys, and any row can be traced back to its bronze file by SHA.
