"""National Grid ESO Carbon Intensity API.

https://carbon-intensity.github.io/api-definitions/

Three daily datasets and one slowly changing reference dataset:

* ``national_intensity``  GET /intensity/{from}/{to}          forecast + actual, 48 half-hours
* ``national_generation`` GET /generation/{from}/{to}        national fuel mix, 48 half-hours
* ``regional_intensity``  GET /regional/intensity/{from}/{to} forecast + fuel mix for 18 regions
* ``carbon_factors``      GET /intensity/factors             gCO2/kWh per fuel type

Two quirks the rest of the pipeline is built around:

1. Ranges are capped at 14 days, so extraction is per calendar day.
2. A request for ``{day}T00:00Z`` returns the half-hour that *contains* that
   instant, i.e. ``{day-1}T23:30Z``, so a "day" arrives as 49 periods. The
   silver parser trims to the partition window; bronze keeps the raw 49.
"""

from __future__ import annotations

from collections.abc import Iterator

from gridcarbon.config import Settings, get_settings
from gridcarbon.domain import Partition
from gridcarbon.http import HttpClient
from gridcarbon.sources.base import RawFetch

SOURCE_NAME = "carbon_intensity"
DAILY_DATASETS: tuple[str, ...] = (
    "national_intensity",
    "national_generation",
    "regional_intensity",
)
REFERENCE_DATASETS: tuple[str, ...] = ("carbon_factors",)


class CarbonIntensitySource:
    name = SOURCE_NAME
    datasets = DAILY_DATASETS + REFERENCE_DATASETS

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def _url(self, dataset: str, partition: Partition) -> str:
        base = self.settings.carbon_intensity_base_url.rstrip("/")
        window = f"{partition.api_from}/{partition.api_to}"
        match dataset:
            case "national_intensity":
                return f"{base}/intensity/{window}"
            case "national_generation":
                return f"{base}/generation/{window}"
            case "regional_intensity":
                return f"{base}/regional/intensity/{window}"
            case "carbon_factors":
                return f"{base}/intensity/factors"
        raise ValueError(f"unknown dataset {dataset!r} for {self.name}")

    def fetch(self, client: HttpClient, partition: Partition, dataset: str) -> Iterator[RawFetch]:
        url = self._url(dataset, partition)
        result = client.get(url)
        yield RawFetch(
            source=self.name,
            dataset=dataset,
            partition=partition,
            result=result,
            request={"url": url, "from": partition.api_from, "to": partition.api_to},
        )
