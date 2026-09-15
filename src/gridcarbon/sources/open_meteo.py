"""Open-Meteo historical weather API (ERA5 reanalysis, keyless).

https://open-meteo.com/en/docs/historical-weather-api

One request per partition fetches hourly weather for every DNO region's
representative point at once (the API accepts comma-separated coordinate
lists and returns one object per location with a zero-based ``location_id``).
The request metadata records the region order so the parser can map
``location_id`` back to ``region_id`` without trusting float equality on the
coordinates the API echoes back (it snaps them to its grid).
"""

from __future__ import annotations

from collections.abc import Iterator

from gridcarbon.config import Settings, get_settings
from gridcarbon.domain import DNO_REGIONS, WEATHER_VARIABLES, Partition
from gridcarbon.http import HttpClient
from gridcarbon.sources.base import RawFetch

SOURCE_NAME = "open_meteo"
DAILY_DATASETS: tuple[str, ...] = ("weather_hourly",)


class OpenMeteoSource:
    name = SOURCE_NAME
    datasets = DAILY_DATASETS

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def fetch(self, client: HttpClient, partition: Partition, dataset: str) -> Iterator[RawFetch]:
        if dataset not in self.datasets:
            raise ValueError(f"unknown dataset {dataset!r} for {self.name}")
        region_ids = [r.region_id for r in DNO_REGIONS]
        params = {
            "latitude": ",".join(f"{r.latitude:.4f}" for r in DNO_REGIONS),
            "longitude": ",".join(f"{r.longitude:.4f}" for r in DNO_REGIONS),
            "start_date": partition.key,
            "end_date": partition.key,
            "hourly": ",".join(WEATHER_VARIABLES),
            "timezone": "UTC",
            "wind_speed_unit": "ms",
        }
        result = client.get(self.settings.open_meteo_base_url, params=params)
        yield RawFetch(
            source=self.name,
            dataset=dataset,
            partition=partition,
            result=result,
            request={"params": params, "region_ids": region_ids},
        )
