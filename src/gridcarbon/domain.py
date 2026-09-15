"""Domain constants shared by every layer: partitions, regions and fuels.

The 18 regions are the ones the Carbon Intensity API reports. IDs 1 to 14 are
Distribution Network Operator (DNO) licence areas; 15 to 18 are aggregates that
the API also returns and which we keep as rows but never geocode. Coordinates
are a representative point inside each DNO area (a major settlement), not a
computed geometric centroid, and are documented as such in the data dictionary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Final


@dataclass(frozen=True, order=True)
class Partition:
    """One UTC calendar day. The unit of extraction, validation and rewrite."""

    day: date

    @classmethod
    def parse(cls, value: str | date) -> Partition:
        if isinstance(value, date):
            return cls(value)
        return cls(date.fromisoformat(value))

    @property
    def key(self) -> str:
        return self.day.isoformat()

    @property
    def start(self) -> datetime:
        return datetime(self.day.year, self.day.month, self.day.day, tzinfo=UTC)

    @property
    def end(self) -> datetime:
        return self.start + timedelta(days=1)

    @property
    def api_from(self) -> str:
        """Carbon Intensity API timestamps: ISO-8601 to the minute, trailing Z."""
        return self.start.strftime("%Y-%m-%dT%H:%MZ")

    @property
    def api_to(self) -> str:
        return self.end.strftime("%Y-%m-%dT%H:%MZ")

    def __str__(self) -> str:
        return self.key

    def next(self) -> Partition:
        return Partition(self.day + timedelta(days=1))

    def previous(self) -> Partition:
        return Partition(self.day - timedelta(days=1))


def partition_range(start: date | str, end: date | str) -> list[Partition]:
    """Inclusive range of daily partitions."""
    first = Partition.parse(start)
    last = Partition.parse(end)
    if last < first:
        raise ValueError(f"end {last} is before start {first}")
    out: list[Partition] = []
    cursor = first
    while cursor <= last:
        out.append(cursor)
        cursor = cursor.next()
    return out


@dataclass(frozen=True)
class Region:
    region_id: int
    short_name: str
    dno_region: str | None
    latitude: float | None
    longitude: float | None
    is_aggregate: bool

    @property
    def has_location(self) -> bool:
        return self.latitude is not None and self.longitude is not None


REGIONS: Final[tuple[Region, ...]] = (
    Region(1, "North Scotland", "Scottish Hydro Electric Power Distribution", 57.48, -4.22, False),
    Region(2, "South Scotland", "SP Distribution", 55.95, -3.78, False),
    Region(3, "North West England", "Electricity North West", 53.48, -2.24, False),
    Region(4, "North East England", "Northern Powergrid North East", 54.98, -1.61, False),
    Region(5, "Yorkshire", "Northern Powergrid Yorkshire", 53.80, -1.55, False),
    Region(6, "North Wales & Merseyside", "SP Manweb", 53.19, -2.89, False),
    Region(7, "South Wales", "WPD South Wales", 51.48, -3.18, False),
    Region(8, "West Midlands", "WPD West Midlands", 52.48, -1.90, False),
    Region(9, "East Midlands", "WPD East Midlands", 52.95, -1.15, False),
    Region(10, "East England", "UK Power Networks Eastern", 52.21, 0.12, False),
    Region(11, "South West England", "WPD South West", 50.72, -3.53, False),
    Region(12, "South England", "Southern Electric Power Distribution", 50.91, -1.40, False),
    Region(13, "London", "UK Power Networks London", 51.51, -0.13, False),
    Region(14, "South East England", "UK Power Networks South Eastern", 51.27, 0.52, False),
    Region(15, "England", None, None, None, True),
    Region(16, "Scotland", None, None, None, True),
    Region(17, "Wales", None, None, None, True),
    Region(18, "GB", None, None, None, True),
)

REGION_BY_ID: Final[dict[int, Region]] = {r.region_id: r for r in REGIONS}
DNO_REGIONS: Final[tuple[Region, ...]] = tuple(r for r in REGIONS if not r.is_aggregate)

FUELS: Final[tuple[str, ...]] = (
    "biomass",
    "coal",
    "imports",
    "gas",
    "nuclear",
    "other",
    "hydro",
    "solar",
    "wind",
)

INTENSITY_INDEXES: Final[tuple[str, ...]] = ("very low", "low", "moderate", "high", "very high")

WEATHER_VARIABLES: Final[tuple[str, ...]] = (
    "temperature_2m",
    "wind_speed_10m",
    "wind_speed_100m",
    "shortwave_radiation",
    "cloud_cover",
    "precipitation",
)
