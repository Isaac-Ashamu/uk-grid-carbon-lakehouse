"""Runtime configuration.

Everything the pipeline needs to know about its environment lives here and is
overridable with ``GRIDCARBON_*`` environment variables or a ``.env`` file, so
the same code runs unchanged on a laptop, inside the Airflow container and in CI.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="GRIDCARBON_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # ---- storage -----------------------------------------------------------
    data_root: Path = Field(
        default=Path("data"),
        description="Root of the local lake: bronze/, silver/, warehouse/, audit/ live under it.",
    )

    # ---- sources -----------------------------------------------------------
    carbon_intensity_base_url: str = "https://api.carbonintensity.org.uk"
    open_meteo_base_url: str = "https://archive-api.open-meteo.com/v1/archive"
    http_timeout_seconds: float = 30.0
    http_max_attempts: int = 5
    http_min_backoff_seconds: float = 0.5
    http_max_backoff_seconds: float = 20.0
    http_rate_limit_backoff_seconds: float = Field(
        default=15.0,
        description="Base wait after an HTTP 429, multiplied by the attempt number (cap 120s).",
    )
    http_min_interval_seconds: float = Field(
        default=0.6,
        description="Client-side throttle: minimum seconds between two requests to the same host.",
    )
    http_user_agent: str = (
        "gridcarbon/0.1 (+https://github.com/Isaac-Ashamu/uk-grid-carbon-lakehouse)"
    )

    # ---- serving -----------------------------------------------------------
    postgres_dsn: str | None = Field(
        default=None,
        description="psycopg DSN for the serving database, e.g. postgresql://user:pw@host:5432/db",
    )
    postgres_schema: str = "marts"
    api_backend: str = Field(
        default="duckdb",
        description="Store the read API queries: 'duckdb' (the warehouse file) or 'postgres'.",
    )

    # ---- dbt ---------------------------------------------------------------
    dbt_project_dir: Path = Field(default=Path("dbt"))
    dbt_target: str = "dev"

    # ---- quality thresholds ------------------------------------------------
    min_periods_per_day: int = Field(
        default=44,
        description=(
            "A day has 48 half-hour settlement periods. Fewer than this many after "
            "validation fails the partition rather than silently publishing a thin day."
        ),
    )
    generation_mix_tolerance_pct: float = Field(
        default=2.0,
        description="A generation mix whose percentages sum outside 100 +/- this is quarantined.",
    )

    # ---- derived paths -----------------------------------------------------
    @property
    def bronze_dir(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_root / "silver"

    @property
    def warehouse_dir(self) -> Path:
        return self.data_root / "warehouse"

    @property
    def warehouse_path(self) -> Path:
        return self.warehouse_dir / "gridcarbon.duckdb"

    @property
    def audit_dir(self) -> Path:
        return self.data_root / "audit"

    def ensure_dirs(self) -> None:
        for d in (self.bronze_dir, self.silver_dir, self.warehouse_dir, self.audit_dir):
            d.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    """Tests swap environment variables and need a fresh Settings object."""
    get_settings.cache_clear()
