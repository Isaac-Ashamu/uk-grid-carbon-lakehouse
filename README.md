# uk-grid-carbon-lakehouse

[![ci](https://github.com/Isaac-Ashamu/uk-grid-carbon-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Isaac-Ashamu/uk-grid-carbon-lakehouse/actions/workflows/ci.yml)
[![nightly-live-run](https://github.com/Isaac-Ashamu/uk-grid-carbon-lakehouse/actions/workflows/nightly.yml/badge.svg)](https://github.com/Isaac-Ashamu/uk-grid-carbon-lakehouse/actions/workflows/nightly.yml)
![python](https://img.shields.io/badge/python-3.12-blue)
![dbt](https://img.shields.io/badge/dbt-1.12-orange)
![airflow](https://img.shields.io/badge/airflow-2.10-017CEE)
![license](https://img.shields.io/badge/license-MIT-green)

A production-shaped **medallion lakehouse** over two keyless UK open-data APIs:
National Grid ESO's [Carbon Intensity API](https://carbon-intensity.github.io/api-definitions/)
(half-hourly carbon intensity and generation mix for GB and its 14 distribution regions) and
[Open-Meteo](https://open-meteo.com/en/docs/historical-weather-api) (hourly ERA5 weather at
each region's representative point).

It answers two questions a grid analyst would actually ask: **how well does ESO's day-ahead
intensity forecast perform, and when?** and **how much does local weather explain the
generation mix and carbon intensity of each region?** — and it does so with every
engineering control you would expect in a real platform: immutable raw storage with
provenance, typed contracts with a quarantine, SQL-defined marts with tests, idempotent
partition rewrites, resumable backfills, an audit trail, orchestration with a quality gate,
a serving database, a read API, CI, and a nightly live run.

```text
Carbon Intensity API ─┐                                        ┌─ Postgres (COPY + upsert / swap)
                      ├─► bronze ─► silver ─► DuckDB ─► dbt marts ─┤
Open-Meteo archive ───┘   raw JSON   Parquet   views    tables     └─ FastAPI read API
                          + SHA-256  + pandera            + tests
                          manifests  + quarantine         + docs
                                           ▲
                        Airflow: daily DAG (catchup), quality gate, dataset-triggered smoke
```

## What is in the box

| Layer | What it does | Where |
|---|---|---|
| **Extract** | httpx client with retry/backoff/jitter, `Retry-After`, 429-aware waits, per-host throttle; one request per dataset per UTC day | `src/gridcarbon/http.py`, `sources/` |
| **Bronze** | Raw bytes as received, Hive-partitioned, append-only, SHA-256 dedup, manifest sidecar (URL, params, status, size, timing, run id) | `bronze.py` |
| **Silver** | Pydantic structural parsing → pandera value contracts → zstd Parquet, partition overwrite; rejects go to a `quarantine` table with reason + record; thin days fail the run | `silver/` |
| **Warehouse** | DuckDB file with `silver.*` views over the Parquet (one copy of the data) and `audit.pipeline_runs` over the run log; UTC forced on every connection | `warehouse.py` |
| **Gold** | dbt-duckdb: 6 staging views (latest fetch wins), 3 intermediate (pivot, half-hour weather interpolation), 11 marts, 2 seeds, 57 schema/source tests + 4 singular tests, generic `value_between` / `row_count_at_least` macros | `dbt/` |
| **Serving** | Postgres loader: COPY into temp + `ON CONFLICT DO UPDATE` for keyed facts, atomic rename-swap for recomputed tables; DDL derived from DuckDB's catalogue | `loaders/postgres.py` |
| **API** | FastAPI over DuckDB or Postgres (same SQL, different placeholder style), OpenAPI docs, 404/422/503 semantics | `api/` |
| **Orchestration** | Airflow 2.10: `gridcarbon_daily` (06:00 UTC, catchup, per-dataset tasks, one-slot warehouse pool, quality gate), `gridcarbon_reference_weekly`, dataset-triggered `gridcarbon_api_smoke` | `airflow/dags/` |
| **Ops** | Structured JSON logs bound to `run_id`/`partition`; `data_quality_daily` mart; `gridcarbon runs` / `status` / `query` CLI; runbook; ADRs | `audit.py`, `cli.py`, `docs/` |
| **Delivery** | Docker (runtime + Airflow images), Compose (Postgres + Airflow + API), GitHub Actions (ruff, mypy `--strict`, pytest with a Postgres service, offline dbt build, DAG integrity, Docker builds), nightly live run with quality gate | `Dockerfile`, `docker/`, `.github/workflows/` |

## Quickstart

```bash
git clone https://github.com/Isaac-Ashamu/uk-grid-carbon-lakehouse && cd uk-grid-carbon-lakehouse
make install                                   # uv venv + editable install (Python 3.12)
source .venv/bin/activate

gridcarbon reference                           # snapshot ESO emission factors
gridcarbon backfill 2026-08-01 2026-08-31 --no-load   # 31 days: bronze → silver → dbt build + tests
gridcarbon status                              # what is in the lake
gridcarbon runs --last 12                      # audit trail
gridcarbon query "select date_day, mean_actual_gco2_kwh, forecast_mae_gco2_kwh from marts.agg_daily_national order by 1"
gridcarbon serve                               # http://localhost:8000/docs
```

With Docker (Postgres + Airflow + API, lake shared in `./data`):

```bash
make up                         # docker compose up -d --build
open http://localhost:8080      # Airflow (airflow / airflow) — unpause gridcarbon_daily
open http://localhost:8000/docs # API, served from Postgres
docker compose run --rm cli backfill 2026-08-01 2026-08-07
```

Every setting is an environment variable (`GRIDCARBON_*`, see `.env.example`); nothing needs
an API key.

## The data model

Grain and rules are in [`docs/data-dictionary.md`](docs/data-dictionary.md). The short version:

* **Facts** at the half-hour *settlement period* (48 per UTC day): `fct_intensity_national_halfhourly`
  (forecast, actual, error) and `fct_intensity_regional_halfhourly` (18 regions × forecast ×
  9-fuel mix pivoted wide × weather interpolated to the half hour).
* **Dimensions**: `dim_region` (14 DNO areas + 4 aggregates, with a documented representative
  point rather than a pretend centroid), `dim_fuel` / `dim_fuel_factors` (grouping + ESO gCO₂/kWh
  factors), `dim_date`.
* **Aggregates**: daily national and regional summaries with forecast MAE/bias/MAPE/RMSE;
  forecast accuracy by settlement period; cleanest hour of day per region; wind-speed bins vs
  wind share; Pearson correlations of weather vs generation and intensity; `data_quality_daily`.

## What 90 days of data say (17 Jun – 14 Sep 2026)

Numbers below come straight from the marts after `gridcarbon backfill 2026-06-17 2026-09-14`
(4,320 national periods, 77,760 regional periods, 699,840 generation-mix rows, 30,240 weather
rows; 35 MB of bronze JSON became 5.6 MB of silver Parquet; zero rows quarantined).

* GB averaged **125 gCO₂/kWh**, ranging from 57 (4 July, 63% renewable) to 209 (30 June, 48% gas).
* ESO's day-ahead forecast had an **MAE of 11.9 gCO₂/kWh (MAPE 11%)** with a small negative
  bias (−1.6). It is most accurate late morning (MAE ≈ 6 at 10:00–12:00 UTC) and worst
  mid-afternoon (**MAE 19.8, bias −18.7 at 15:00 UTC**): the forecast systematically
  *under*-estimates afternoon intensity, consistent with over-estimating solar.
* Weather explains the mix: solar radiation vs solar share has **r = 0.81–0.95** in 13 of the
  14 regions (0.58 in North Scotland, where solar is negligible); 100 m wind speed vs wind share reaches **r = 0.72** (East England) and is weakest in
  North Scotland (0.28) — because that region ran at ~100% renewable (0 gCO₂/kWh) regardless.
* In North West England the wind share climbs from **18% at < 2 m/s to 62% at 10–12 m/s** while
  intensity falls from 50 to 14 gCO₂/kWh; its cleanest hours are 10:00–12:00 UTC (27 gCO₂/kWh)
  and dirtiest 19:00–20:00 (53).
* Regional intensity spans an order of magnitude: North Scotland 0, South Scotland 1.5,
  North East England 17 … South West England 243, South Wales 272 gCO₂/kWh.

```bash
curl -s "localhost:8000/regions/3/cleanest-hours" | jq '.[0:3]'
curl -s "localhost:8000/forecast-accuracy" | jq 'max_by(.mae_gco2_kwh)'
curl -s "localhost:8000/weather-correlation" | jq '.[] | {region_name, corr_wind_speed_vs_wind_pct}'
```

## Data quality, by design

* **Structural contract** (pydantic): field names, nesting and types of each API payload. A
  record that does not fit is quarantined with the validation error, never coerced.
* **Value contract** (pandera): ranges (0–1500 gCO₂/kWh, 0–100%), accepted values, key
  uniqueness, exact 30-minute periods. Failing rows are quarantined with the check name;
  the rest of the day is published.
* **Domain rules**: a generation mix must sum to 100 ± 2%; the half hour that *contains*
  `T00:00Z` belongs to the previous day and is trimmed; weather hours with every variable
  null (archive lag) are quarantined and back-filled by the next run.
* **Partition gates**: fewer than 44 of 48 periods, or fewer than half the expected weather
  rows, raise `PartitionQualityError` and fail the stage instead of publishing a thin day.
* **dbt tests**: 57 schema and source tests plus 4 singular tests (mix sums to 100 after pivot; every fully
  loaded day has 48 periods; every period has 18 regions; every DNO period on a complete day has
  weather). The 48-periods test caught a real timezone bug on the first run — see
  [ADR 0002](docs/adr/0002-utc-everywhere.md).
* **Quality gate in the DAG**: `marts.data_quality_daily` is checked before the Postgres load;
  a failed transform or a quarantine rate above 5% fails the run.

## Idempotency and backfills

Every stage can be re-run safely, which is what makes backfills and re-processing boring:

| stage | re-run behaviour |
|---|---|
| extract | skipped if bronze already has the partition (`--force` re-fetches); identical bytes are never stored twice; a changed response is appended and silver takes the latest |
| transform | overwrites exactly that partition's Parquet directory; a clean re-run removes the day's old quarantine file |
| dbt build | full rebuild of views and marts from silver |
| load | upsert by primary key for facts and daily aggregates, atomic table swap for recomputed aggregates |

During the 90-day backfill Open-Meteo began returning 429 after ~70 requests. The run kept
going (86 of 90 days landed), the audit log recorded the 4 failures, and after adding a
per-host throttle and a 429-specific backoff, re-running the same command skipped 356
already-fetched extracts and fetched only the 4 missing days.

## Testing

```bash
make test        # 38 tests: unit (respx-mocked HTTP, recorded fixtures, hypothesis) + integration
make test-dags   # DAG integrity in the Airflow venv
make lint        # ruff, ruff format --check, mypy --strict
```

* `tests/fixtures/2026-09-01/` holds real responses from both APIs. `shift_payload` re-dates
  them, so the mocked transport serves any partition offline — the integration test backfills
  five days, runs `dbt build` (20 models + 2 seeds, 61 tests), hits every API endpoint, and, when
  `GRIDCARBON_POSTGRES_DSN` is set (CI provides a Postgres service), loads Postgres twice and
  proves the loaders are idempotent.
* Unit tests cover retry/backoff/throttle behaviour, bronze dedup and crash-safety (the manifest
  is the commit marker), the containing-period trim, every quarantine path, partition gates,
  silver overwrite semantics, the warehouse view/placeholder swap and a failing partition
  inside a backfill.

## Orchestration

`gridcarbon_daily` (06:00 UTC, `catchup=True`) runs one DAG run per UTC day: 4 extract tasks →
4 transform tasks → `build_marts` → `quality_gate` → `load_postgres`. Extraction parallelises
across up to 3 active runs; `build_marts` and `load_postgres` share a one-slot pool because
DuckDB has a single writer. `build_marts` publishes an Airflow Dataset that triggers
`gridcarbon_api_smoke`. The DAGs are thin: every task calls a function in `gridcarbon.pipeline`
that the tests exercise without Airflow. See [ADR 0003](docs/adr/0003-orchestration-thin-dags.md)
and the [runbook](docs/runbook.md).

## Project layout

```text
src/gridcarbon/
  config.py        pydantic-settings; GRIDCARBON_* env vars
  http.py          retrying, throttled client; FetchResult with sha256
  domain.py        Partition, 18 regions, fuels, weather variables
  sources/         carbon_intensity.py, open_meteo.py, registry
  bronze.py        raw storage + manifests + dedup
  silver/          models.py (pydantic), schemas.py (pandera), parsers.py, writer.py
  warehouse.py     DuckDB views over Parquet, audit view, UTC
  pipeline.py      extract / transform / build_marts / load_serving / run_partition / backfill
  loaders/         postgres.py (COPY + upsert / swap)
  api/             FastAPI app + repository (DuckDB or Postgres)
  audit.py         run log (JSONL) with stage context manager
  cli.py           typer CLI
dbt/               dbt-duckdb project: staging → intermediate → marts, seeds, tests, macros
airflow/dags/      gridcarbon_daily, gridcarbon_reference_weekly, gridcarbon_api_smoke
tests/             unit/, integration/, fixtures/ (recorded API responses)
docs/              data dictionary, runbook, ADRs
docker/, Dockerfile, docker-compose.yml, Makefile, .github/workflows/
```

## Decisions

* [ADR 0001](docs/adr/0001-medallion-on-parquet-and-duckdb.md) — medallion layers on local
  Parquet with DuckDB as the warehouse, Postgres for serving
* [ADR 0002](docs/adr/0002-utc-everywhere.md) — UTC everywhere, and the timezone bug a dbt test caught
* [ADR 0003](docs/adr/0003-orchestration-thin-dags.md) — Airflow with thin DAGs over a testable library

## Roadmap

* Object-storage lake (S3/MinIO) behind the same writer interface; Iceberg table format for silver
* Great Expectations or Soda for consumer-facing data docs alongside the pandera contracts
* Regional *actual* intensity via ESO's regional generation data when it is published
* A small Streamlit page over the API for the "when should I charge" question

## Licence

MIT. Data © National Grid ESO (CC BY 4.0) and Open-Meteo (CC BY 4.0).
