"""Query layer for the API: the same SQL against DuckDB (default) or Postgres.

The marts have identical column names in both stores, so each query is written
once with ``{p}`` placeholders and bound with the backend's parameter style.
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any, Protocol

from gridcarbon.config import Settings, get_settings


def _clean(row: dict[str, Any]) -> dict[str, Any]:
    return {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}


class Repository(Protocol):
    def rows(self, sql: str, params: list[Any]) -> list[dict[str, Any]]: ...
    def close(self) -> None: ...
    @property
    def backend(self) -> str: ...


class DuckDBRepository:
    backend = "duckdb"

    def __init__(self, settings: Settings) -> None:
        import duckdb

        self._con = duckdb.connect(str(settings.warehouse_path), read_only=True)
        self._con.execute("SET TimeZone = 'UTC'")

    def rows(self, sql: str, params: list[Any]) -> list[dict[str, Any]]:
        cur = self._con.execute(sql.replace("{p}", "?"), params)
        cols = [d[0] for d in cur.description or []]
        return [_clean(dict(zip(cols, r, strict=True))) for r in cur.fetchall()]

    def close(self) -> None:
        self._con.close()


class PostgresRepository:
    backend = "postgres"

    def __init__(self, settings: Settings) -> None:
        import psycopg
        from psycopg.rows import dict_row

        if not settings.postgres_dsn:
            raise RuntimeError("GRIDCARBON_POSTGRES_DSN is not set")
        self._con = psycopg.connect(settings.postgres_dsn, row_factory=dict_row, autocommit=True)
        self._schema = settings.postgres_schema

    def rows(self, sql: str, params: list[Any]) -> list[dict[str, Any]]:
        with self._con.cursor() as cur:
            cur.execute(sql.replace("{p}", "%s").replace("marts.", f"{self._schema}."), params)
            return [_clean(dict(r)) for r in cur.fetchall()]

    def close(self) -> None:
        self._con.close()


def open_repository(settings: Settings | None = None, backend: str | None = None) -> Repository:
    settings = settings or get_settings()
    chosen = backend or (
        "postgres" if settings.postgres_dsn and settings.api_backend == "postgres" else "duckdb"
    )
    if chosen == "postgres":
        return PostgresRepository(settings)
    return DuckDBRepository(settings)


# ---- queries -----------------------------------------------------------------


def regions(repo: Repository) -> list[dict[str, Any]]:
    return repo.rows(
        "SELECT region_id, region_name, dno_region, nation, latitude, longitude, is_aggregate "
        "FROM marts.dim_region ORDER BY region_id",
        [],
    )


def national_halfhourly(repo: Repository, day: date) -> list[dict[str, Any]]:
    return repo.rows(
        "SELECT period_start, period_end, settlement_period, forecast_gco2_kwh, actual_gco2_kwh, "
        "intensity_index, forecast_error_gco2_kwh, renewable_pct, low_carbon_pct, fossil_pct, "
        "wind_pct, solar_pct, gas_pct, nuclear_pct, imports_pct "
        "FROM marts.fct_intensity_national_halfhourly WHERE date_day = {p} ORDER BY period_start",
        [day],
    )


def regional_halfhourly(repo: Repository, region_id: int, day: date) -> list[dict[str, Any]]:
    return repo.rows(
        "SELECT period_start, period_end, settlement_period, region_id, region_name, "
        "forecast_gco2_kwh, intensity_index, renewable_pct, low_carbon_pct, fossil_pct, wind_pct, "
        "solar_pct, gas_pct, nuclear_pct, imports_pct, temperature_c, wind_speed_100m_ms, "
        "shortwave_radiation_wm2, cloud_cover_pct, precipitation_mm, weather_method "
        "FROM marts.fct_intensity_regional_halfhourly "
        "WHERE region_id = {p} AND date_day = {p} ORDER BY period_start",
        [region_id, day],
    )


def daily_national(repo: Repository, start: date, end: date) -> list[dict[str, Any]]:
    return repo.rows(
        "SELECT * FROM marts.agg_daily_national "
        "WHERE date_day BETWEEN {p} AND {p} ORDER BY date_day",
        [start, end],
    )


def daily_regional(
    repo: Repository, region_id: int, start: date, end: date
) -> list[dict[str, Any]]:
    return repo.rows(
        "SELECT * FROM marts.agg_daily_regional "
        "WHERE region_id = {p} AND date_day BETWEEN {p} AND {p} ORDER BY date_day",
        [region_id, start, end],
    )


def cleanest_hours(repo: Repository, region_id: int) -> list[dict[str, Any]]:
    return repo.rows(
        "SELECT hour_of_day_utc, periods, mean_forecast_gco2_kwh, mean_renewable_pct, "
        "mean_low_carbon_pct, mean_fossil_pct, cleanliness_rank "
        "FROM marts.agg_hour_of_day_regional WHERE region_id = {p} ORDER BY cleanliness_rank",
        [region_id],
    )


def forecast_accuracy(repo: Repository) -> list[dict[str, Any]]:
    return repo.rows(
        "SELECT * FROM marts.agg_forecast_accuracy_by_settlement_period ORDER BY settlement_period",
        [],
    )


def weather_correlation(repo: Repository) -> list[dict[str, Any]]:
    return repo.rows("SELECT * FROM marts.agg_weather_intensity_correlation ORDER BY region_id", [])


def wind_speed_bins(repo: Repository, region_id: int) -> list[dict[str, Any]]:
    return repo.rows(
        "SELECT * FROM marts.agg_wind_speed_bins_regional WHERE region_id = {p} "
        "ORDER BY wind_speed_bin_floor_ms",
        [region_id],
    )


def quality(repo: Repository, limit: int) -> list[dict[str, Any]]:
    return repo.rows(
        "SELECT * FROM marts.data_quality_daily ORDER BY date_day DESC, dataset LIMIT {p}", [limit]
    )


def coverage(repo: Repository) -> dict[str, Any]:
    rows = repo.rows(
        "SELECT min(date_day) AS first_day, max(date_day) AS last_day, count(*) AS days, "
        "sum(CASE WHEN is_complete_day THEN 1 ELSE 0 END) AS complete_days "
        "FROM marts.agg_daily_national",
        [],
    )
    return rows[0] if rows else {"first_day": None, "last_day": None, "days": 0, "complete_days": 0}
