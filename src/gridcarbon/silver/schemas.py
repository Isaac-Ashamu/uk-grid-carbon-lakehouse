"""Silver data contracts, expressed as pandera schemas.

Each silver table has exactly one schema. The schema is the single source of
truth for column names, dtypes, nullability, value ranges and key uniqueness;
the Parquet writer, the dbt sources and the data dictionary all follow it.
"""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa

from gridcarbon.domain import FUELS, INTENSITY_INDEXES

_LINEAGE_COLUMNS: dict[str, pa.Column] = {
    "partition_date": pa.Column("datetime64[ns]", nullable=False),
    "fetched_at": pa.Column("datetime64[ns, UTC]", nullable=False),
    "run_id": pa.Column(str, nullable=False),
    "bronze_sha256": pa.Column(str, nullable=False, checks=pa.Check.str_length(64, 64)),
}

_TS = "datetime64[ns, UTC]"


def _period_columns() -> dict[str, pa.Column]:
    return {
        "period_start": pa.Column(_TS, nullable=False),
        "period_end": pa.Column(_TS, nullable=False),
    }


def _half_hour_periods(df: pd.DataFrame) -> pd.Series:
    return (df["period_end"] - df["period_start"]) == pd.Timedelta(minutes=30)


INTENSITY_NATIONAL = pa.DataFrameSchema(
    {
        **_period_columns(),
        "forecast": pa.Column("Int64", nullable=True, checks=pa.Check.in_range(0, 1500)),
        "actual": pa.Column("Int64", nullable=True, checks=pa.Check.in_range(0, 1500)),
        "index": pa.Column(str, nullable=False, checks=pa.Check.isin(INTENSITY_INDEXES)),
        **_LINEAGE_COLUMNS,
    },
    checks=[pa.Check(_half_hour_periods, error="period must be exactly 30 minutes")],
    unique=["period_start"],
    name="intensity_national",
    strict=True,
    coerce=True,
)

GENERATION_NATIONAL = pa.DataFrameSchema(
    {
        **_period_columns(),
        "fuel": pa.Column(str, nullable=False, checks=pa.Check.isin(FUELS)),
        "perc": pa.Column(float, nullable=False, checks=pa.Check.in_range(0.0, 100.0)),
        **_LINEAGE_COLUMNS,
    },
    checks=[pa.Check(_half_hour_periods, error="period must be exactly 30 minutes")],
    unique=["period_start", "fuel"],
    name="generation_national",
    strict=True,
    coerce=True,
)

INTENSITY_REGIONAL = pa.DataFrameSchema(
    {
        **_period_columns(),
        "region_id": pa.Column("int64", nullable=False, checks=pa.Check.in_range(1, 18)),
        "forecast": pa.Column("Int64", nullable=True, checks=pa.Check.in_range(0, 1500)),
        "index": pa.Column(str, nullable=False, checks=pa.Check.isin(INTENSITY_INDEXES)),
        **_LINEAGE_COLUMNS,
    },
    checks=[pa.Check(_half_hour_periods, error="period must be exactly 30 minutes")],
    unique=["period_start", "region_id"],
    name="intensity_regional",
    strict=True,
    coerce=True,
)

GENERATION_REGIONAL = pa.DataFrameSchema(
    {
        **_period_columns(),
        "region_id": pa.Column("int64", nullable=False, checks=pa.Check.in_range(1, 18)),
        "fuel": pa.Column(str, nullable=False, checks=pa.Check.isin(FUELS)),
        "perc": pa.Column(float, nullable=False, checks=pa.Check.in_range(0.0, 100.0)),
        **_LINEAGE_COLUMNS,
    },
    checks=[pa.Check(_half_hour_periods, error="period must be exactly 30 minutes")],
    unique=["period_start", "region_id", "fuel"],
    name="generation_regional",
    strict=True,
    coerce=True,
)

WEATHER_HOURLY = pa.DataFrameSchema(
    {
        "region_id": pa.Column("int64", nullable=False, checks=pa.Check.in_range(1, 14)),
        "observed_at": pa.Column(_TS, nullable=False),
        "temperature_2m": pa.Column(float, nullable=True, checks=pa.Check.in_range(-40.0, 50.0)),
        "wind_speed_10m": pa.Column(float, nullable=True, checks=pa.Check.in_range(0.0, 80.0)),
        "wind_speed_100m": pa.Column(float, nullable=True, checks=pa.Check.in_range(0.0, 100.0)),
        "shortwave_radiation": pa.Column(
            float, nullable=True, checks=pa.Check.in_range(0.0, 1400.0)
        ),
        "cloud_cover": pa.Column(float, nullable=True, checks=pa.Check.in_range(0.0, 100.0)),
        "precipitation": pa.Column(float, nullable=True, checks=pa.Check.in_range(0.0, 200.0)),
        **_LINEAGE_COLUMNS,
    },
    unique=["region_id", "observed_at"],
    name="weather_hourly",
    strict=True,
    coerce=True,
)

CARBON_FACTORS = pa.DataFrameSchema(
    {
        "fuel_label": pa.Column(str, nullable=False),
        "gco2_per_kwh": pa.Column("int64", nullable=False, checks=pa.Check.in_range(0, 1500)),
        **_LINEAGE_COLUMNS,
    },
    unique=["fuel_label", "partition_date"],
    name="carbon_factors",
    strict=True,
    coerce=True,
)

QUARANTINE = pa.DataFrameSchema(
    {
        "table": pa.Column(str, nullable=False),
        "reason": pa.Column(str, nullable=False),
        "record": pa.Column(str, nullable=False),
        **_LINEAGE_COLUMNS,
    },
    name="quarantine",
    strict=True,
    coerce=True,
)

SCHEMAS: dict[str, pa.DataFrameSchema] = {
    "intensity_national": INTENSITY_NATIONAL,
    "generation_national": GENERATION_NATIONAL,
    "intensity_regional": INTENSITY_REGIONAL,
    "generation_regional": GENERATION_REGIONAL,
    "weather_hourly": WEATHER_HOURLY,
    "carbon_factors": CARBON_FACTORS,
    "quarantine": QUARANTINE,
}

# Which silver tables each bronze dataset produces.
DATASET_TABLES: dict[str, tuple[str, ...]] = {
    "national_intensity": ("intensity_national",),
    "national_generation": ("generation_national",),
    "regional_intensity": ("intensity_regional", "generation_regional"),
    "weather_hourly": ("weather_hourly",),
    "carbon_factors": ("carbon_factors",),
}
