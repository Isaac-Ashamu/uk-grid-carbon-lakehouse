"""Registry of sources and the datasets each one produces."""

from __future__ import annotations

from gridcarbon.config import Settings
from gridcarbon.sources.base import RawFetch, Source
from gridcarbon.sources.carbon_intensity import (
    DAILY_DATASETS as CARBON_DAILY,
)
from gridcarbon.sources.carbon_intensity import (
    REFERENCE_DATASETS as CARBON_REFERENCE,
)
from gridcarbon.sources.carbon_intensity import (
    CarbonIntensitySource,
)
from gridcarbon.sources.open_meteo import DAILY_DATASETS as METEO_DAILY
from gridcarbon.sources.open_meteo import OpenMeteoSource

__all__ = [
    "DAILY_DATASETS",
    "REFERENCE_DATASETS",
    "RawFetch",
    "Source",
    "build_sources",
    "source_for_dataset",
]

# (source name, dataset) pairs in the order the daily DAG runs them.
DAILY_DATASETS: tuple[tuple[str, str], ...] = tuple(
    [("carbon_intensity", d) for d in CARBON_DAILY] + [("open_meteo", d) for d in METEO_DAILY]
)
REFERENCE_DATASETS: tuple[tuple[str, str], ...] = tuple(
    ("carbon_intensity", d) for d in CARBON_REFERENCE
)
ALL_DATASETS: tuple[tuple[str, str], ...] = DAILY_DATASETS + REFERENCE_DATASETS


def build_sources(settings: Settings | None = None) -> dict[str, Source]:
    return {
        "carbon_intensity": CarbonIntensitySource(settings),
        "open_meteo": OpenMeteoSource(settings),
    }


def source_for_dataset(dataset: str) -> str:
    for source, name in ALL_DATASETS:
        if name == dataset:
            return source
    raise KeyError(f"no source produces dataset {dataset!r}")
