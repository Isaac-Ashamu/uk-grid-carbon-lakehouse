"""Pydantic models for the raw API payloads.

These are the *structural* contract with each upstream API: field names,
types and nesting. A record that does not fit is quarantined with the
validation error, not coerced. Value-level rules (ranges, uniqueness, sums)
belong to the pandera schemas in ``schemas.py``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

IntensityIndex = Literal["very low", "low", "moderate", "high", "very high"]


def _parse_ci_timestamp(value: str) -> datetime:
    # The API writes "2026-09-01T00:30Z"; fromisoformat needs "+00:00".
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed


class _Strict(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=False, frozen=True)


class Intensity(_Strict):
    forecast: int | None = None
    actual: int | None = None
    index: IntensityIndex


class FuelShare(_Strict):
    fuel: str
    perc: float = Field(ge=0)

    @field_validator("fuel")
    @classmethod
    def _lower(cls, value: str) -> str:
        return value.strip().lower()


class Period(_Strict):
    from_: Annotated[datetime, Field(alias="from")]
    to: datetime

    @field_validator("from_", "to", mode="before")
    @classmethod
    def _ts(cls, value: object) -> object:
        return _parse_ci_timestamp(value) if isinstance(value, str) else value


class NationalIntensityPeriod(Period):
    intensity: Intensity


class NationalGenerationPeriod(Period):
    generationmix: list[FuelShare]


class RegionalEntry(_Strict):
    regionid: int = Field(ge=1)
    dnoregion: str | None = None
    shortname: str
    intensity: Intensity
    generationmix: list[FuelShare]


class RegionalIntensityPeriod(Period):
    regions: list[RegionalEntry]


class DataEnvelope(_Strict):
    """Every Carbon Intensity endpoint wraps its payload in ``{"data": [...]}``."""

    data: list[dict[str, object]]


class OpenMeteoHourly(_Strict):
    time: list[str]
    temperature_2m: list[float | None]
    wind_speed_10m: list[float | None]
    wind_speed_100m: list[float | None]
    shortwave_radiation: list[float | None]
    cloud_cover: list[float | None]
    precipitation: list[float | None]


class OpenMeteoLocation(_Strict):
    location_id: int = Field(default=0, ge=0)
    latitude: float
    longitude: float
    elevation: float | None = None
    hourly: OpenMeteoHourly
