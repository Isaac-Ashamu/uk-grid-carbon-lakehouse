# Data dictionary

All timestamps are UTC. A *settlement period* is one of the 48 half hours of a UTC
day, numbered 1 to 48. `gCO2/kWh` is grams of CO2 equivalent per kilowatt hour.

## Bronze (`data/bronze/source=<source>/dataset=<dataset>/date=<day>/`)

| dataset | source | endpoint | shape |
|---|---|---|---|
| `national_intensity` | carbon_intensity | `GET /intensity/{day}T00:00Z/{day+1}T00:00Z` | 49 periods (the one containing 00:00Z belongs to the previous day) |
| `national_generation` | carbon_intensity | `GET /generation/{from}/{to}` | 49 periods × 9 fuels |
| `regional_intensity` | carbon_intensity | `GET /regional/intensity/{from}/{to}` | 49 periods × 18 regions × (forecast + 9 fuels) |
| `weather_hourly` | open_meteo | `GET /v1/archive?latitude=…&longitude=…&hourly=…` | 14 locations × 24 hours |
| `carbon_factors` | carbon_intensity | `GET /intensity/factors` | 14 fuel labels (reference; partition = snapshot day) |

Each file has a `.manifest.json` sidecar: `url`, `request`, `status_code`, `bytes`,
`sha256`, `fetched_at`, `elapsed_ms`, `attempts`, `run_id`.

## Silver (`data/silver/<table>/date=<day>/part-0.parquet`)

Every table carries lineage columns: `partition_date`, `fetched_at`, `run_id`, `bronze_sha256`.

### `intensity_national` — key `period_start`
| column | type | rule |
|---|---|---|
| period_start, period_end | timestamptz | exactly 30 minutes apart; within the partition day |
| forecast | int, nullable | 0–1500 |
| actual | int, nullable | 0–1500 (null for periods not yet settled) |
| index | text | very low / low / moderate / high / very high |

### `generation_national` — key `(period_start, fuel)`
| column | type | rule |
|---|---|---|
| fuel | text | biomass, coal, imports, gas, nuclear, other, hydro, solar, wind |
| perc | double | 0–100; a period's fuels must sum to 100 ± 2 or the period is quarantined |

### `intensity_regional` — key `(period_start, region_id)`
As `intensity_national` plus `region_id` (1–18); regional data has forecast only.

### `generation_regional` — key `(period_start, region_id, fuel)`
As `generation_national` plus `region_id`.

### `weather_hourly` — key `(region_id, observed_at)`
| column | unit | rule |
|---|---|---|
| temperature_2m | °C | −40–50 |
| wind_speed_10m, wind_speed_100m | m/s | 0–80, 0–100 |
| shortwave_radiation | W/m² | 0–1400 |
| cloud_cover | % | 0–100 |
| precipitation | mm | 0–200 |

Rows where every variable is null (archive not yet available) are quarantined.
Only DNO regions 1–14 have weather; aggregates 15–18 have no location.

### `carbon_factors` — key `(fuel_label, partition_date)`
`fuel_label` is ESO's label (e.g. "Gas (Combined Cycle)"), `gco2_per_kwh` int.

### `quarantine`
`table`, `reason` (pydantic error, pandera check name, or a rule name), `record` (JSON of the rejected input), lineage.

## Gold (`marts.*` in DuckDB, mirrored to Postgres)

| table | grain | mode in Postgres | notes |
|---|---|---|---|
| `dim_region` | region | swap | Seed. `latitude`/`longitude` are a representative point inside each DNO area (a major settlement), **not** a geometric centroid. |
| `dim_fuel` | fuel | swap | Seed. Groups: renewable (wind, solar, hydro, biomass), low_carbon (+ nuclear), fossil (gas, coal), imports, other. |
| `dim_fuel_factors` | fuel | swap | `dim_fuel` + latest ESO emission factor; imports left null (ESO publishes Dutch/French/Irish separately). |
| `dim_date` | day | upsert | Generated from loaded history; `season`, `is_weekend`, `iso_week`. |
| `fct_intensity_national_halfhourly` | period | upsert | forecast, actual, error (forecast − actual), abs error, % error, pivoted mix, group shares. |
| `fct_intensity_regional_halfhourly` | period × region | upsert | forecast, pivoted mix, group shares, weather at the region point (`weather_method`: observed on the hour, interpolated on the half hour, carried_forward at day end). |
| `agg_daily_national` | day | upsert | mean/min/max, cleanest and dirtiest period, MAE, bias, MAPE, RMSE, mean group shares, `is_complete_day`. |
| `agg_daily_regional` | day × region | upsert | as above plus mean temperature, wind, radiation, cloud, total precipitation. |
| `agg_forecast_accuracy_by_settlement_period` | settlement period | swap | MAE, bias, MAPE, RMSE, median and p90 absolute error, over all history. |
| `agg_hour_of_day_regional` | region × UTC hour | swap | mean intensity and shares; `cleanliness_rank` 1 = cleanest hour. |
| `agg_wind_speed_bins_regional` | region × 2 m/s bin of 100 m wind | swap | mean wind share, renewable share, intensity per bin. |
| `agg_weather_intensity_correlation` | region | swap | Pearson r: wind speed vs wind share / intensity, radiation vs solar share / intensity, temperature vs intensity. NULL (not NaN) when a series has zero variance. |
| `data_quality_daily` | day × dataset | upsert | Latest extract/transform status, rows in/out/quarantined, quarantine rate, durations, whether the national day has all 48 periods (NULL for reference datasets). |

## Audit (`audit.pipeline_runs`, from `data/audit/runs.jsonl`)

One row per stage execution: `run_id`, `stage` (extract, transform, dbt_build,
load_postgres), `dataset`, `partition`, `started_at`, `finished_at`, `status`
(succeeded, failed, skipped), `duration_ms`, `rows_in`, `rows_out`,
`rows_quarantined`, `error`, `details` (JSON: bytes, dedup flag, dbt result
counts, trimmed-row counts, traceback tail on failure).
