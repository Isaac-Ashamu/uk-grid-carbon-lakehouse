# Runbook

## Daily operation

`gridcarbon_daily` runs at 06:00 UTC for the previous UTC day. A healthy run
shows 12 green tasks and `gridcarbon_api_smoke` firing afterwards. Check:

```bash
gridcarbon runs --last 20
gridcarbon query "select * from marts.data_quality_daily order by date_day desc, dataset limit 10"
curl -s localhost:8000/health
```

## Backfill

```bash
# CLI (extract + transform each day, then one dbt build and one Postgres load)
gridcarbon backfill 2026-06-01 2026-06-30

# Airflow (one DAG run per day; extraction parallel, warehouse serialised by pool)
airflow dags backfill gridcarbon_daily --start-date 2026-06-01 --end-date 2026-06-30
```

Both are safe to re-run: bronze deduplicates identical responses, silver
overwrites the partition, dbt rebuilds, Postgres upserts by key.

## Re-processing a day after a parser fix

Bronze already has the raw response, so no API call is needed:

```bash
gridcarbon transform 2026-08-14
gridcarbon build && gridcarbon load
```

## Failure modes

| symptom | likely cause | action |
|---|---|---|
| `extract_*` fails after 5 attempts with `HTTP 5xx` | upstream outage | let Airflow retry (exponential backoff up to 30 min); the other datasets are unaffected |
| `transform_*` raises `PartitionQualityError: only N distinct periods` | API returned a thin day (ESO occasionally backfills late) | re-run the extract task later; bronze keeps every fetch and silver uses the latest |
| `transform_weather_hourly` quarantines all-null hours | Open-Meteo archive lags 2–5 days for final reanalysis | expected for the newest day; the day is complete once re-fetched. `weather_method` in the regional fact tells you which rows were interpolated |
| `quality_gate` fails on quarantine rate | an upstream schema change | inspect `silver.quarantine` for the day: `gridcarbon query "select reason, count(*) from silver.quarantine where date = '2026-09-01' group by 1"` |
| `assert_no_missing_settlement_periods` fails in dbt | a day with ≠ 48 periods (or a timezone regression, see ADR 0002) | `gridcarbon query "select date_day, periods from marts.agg_daily_national where not is_complete_day"` |
| `build_marts` waits in the pool | another run holds the single DuckDB writer slot | normal during backfills |
| API returns 503 | warehouse file not built yet | `gridcarbon build` |

## Adding a dataset

1. Add a fetch in `sources/` and register it in `sources/__init__.py`.
2. Add a pydantic model (`silver/models.py`) and a pandera schema (`silver/schemas.py`).
3. Add a parser in `silver/parsers.py` and map it in `parse_dataset` and `DATASET_TABLES`.
4. Record a fixture (`tests/fixtures/<day>/<dataset>.json`) and add tests.
5. Add a dbt source and staging model; extend the marts.
6. The DAG picks the dataset up automatically from `DAILY_DATASETS`.

## Local environment

```bash
make install            # .venv with everything but Airflow
make install-airflow    # .venv-airflow (pinned constraints)
make test lint
make up                 # docker compose: Postgres + Airflow + API
```
