"""Read-only HTTP API over the gold marts.

uvicorn gridcarbon.api.app:app --reload
GET /health
GET /regions
GET /intensity/national?day=2026-09-01
GET /intensity/regional/3?day=2026-09-01
GET /daily/national?start=2026-09-01&end=2026-09-07
GET /daily/regional/3?start=...&end=...
GET /regions/3/cleanest-hours
GET /regions/3/wind-speed-bins
GET /forecast-accuracy
GET /weather-correlation
GET /quality?limit=50
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import date
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel

from gridcarbon import __version__
from gridcarbon.api import repository as q
from gridcarbon.api.repository import Repository, open_repository
from gridcarbon.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield


app = FastAPI(
    title="gridcarbon",
    version=__version__,
    description="GB grid carbon intensity, generation mix and weather, by region and half hour.",
    lifespan=lifespan,
)


def get_repo() -> Iterator[Repository]:
    settings = get_settings()
    if not settings.warehouse_path.exists() and settings.api_backend != "postgres":
        raise HTTPException(
            status_code=503, detail="warehouse not built yet; run `gridcarbon build`"
        )
    repo = open_repository(settings)
    try:
        yield repo
    finally:
        repo.close()


Repo = Annotated[Repository, Depends(get_repo)]


class Health(BaseModel):
    status: str
    version: str
    backend: str
    first_day: date | None
    last_day: date | None
    days: int
    complete_days: int


class Region(BaseModel):
    region_id: int
    region_name: str
    dno_region: str | None
    nation: str
    latitude: float | None
    longitude: float | None
    is_aggregate: bool


def _region_or_404(repo: Repository, region_id: int) -> None:
    if not any(r["region_id"] == region_id for r in q.regions(repo)):
        raise HTTPException(status_code=404, detail=f"unknown region_id {region_id}")


@app.get("/health", response_model=Health, tags=["ops"])
def health(repo: Repo) -> Any:
    cov = q.coverage(repo)
    return Health(
        status="ok",
        version=__version__,
        backend=repo.backend,
        **{k: cov[k] or 0 if k in ("days", "complete_days") else cov[k] for k in cov},
    )


@app.get("/regions", response_model=list[Region], tags=["reference"])
def regions(repo: Repo) -> Any:
    return q.regions(repo)


@app.get("/intensity/national", tags=["intensity"])
def national(
    repo: Repo, day: Annotated[date, Query(description="UTC day")]
) -> list[dict[str, Any]]:
    rows = q.national_halfhourly(repo, day)
    if not rows:
        raise HTTPException(status_code=404, detail=f"no data for {day}")
    return rows


@app.get("/intensity/regional/{region_id}", tags=["intensity"])
def regional(
    repo: Repo, region_id: int, day: Annotated[date, Query(description="UTC day")]
) -> list[dict[str, Any]]:
    _region_or_404(repo, region_id)
    rows = q.regional_halfhourly(repo, region_id, day)
    if not rows:
        raise HTTPException(status_code=404, detail=f"no data for region {region_id} on {day}")
    return rows


@app.get("/daily/national", tags=["daily"])
def daily_national(repo: Repo, start: date, end: date) -> list[dict[str, Any]]:
    if end < start:
        raise HTTPException(status_code=422, detail="end must be on or after start")
    return q.daily_national(repo, start, end)


@app.get("/daily/regional/{region_id}", tags=["daily"])
def daily_regional(repo: Repo, region_id: int, start: date, end: date) -> list[dict[str, Any]]:
    _region_or_404(repo, region_id)
    if end < start:
        raise HTTPException(status_code=422, detail="end must be on or after start")
    return q.daily_regional(repo, region_id, start, end)


@app.get("/regions/{region_id}/cleanest-hours", tags=["insights"])
def cleanest_hours(repo: Repo, region_id: int) -> list[dict[str, Any]]:
    _region_or_404(repo, region_id)
    return q.cleanest_hours(repo, region_id)


@app.get("/regions/{region_id}/wind-speed-bins", tags=["insights"])
def wind_speed_bins(repo: Repo, region_id: int) -> list[dict[str, Any]]:
    _region_or_404(repo, region_id)
    return q.wind_speed_bins(repo, region_id)


@app.get("/forecast-accuracy", tags=["insights"])
def forecast_accuracy(repo: Repo) -> list[dict[str, Any]]:
    return q.forecast_accuracy(repo)


@app.get("/weather-correlation", tags=["insights"])
def weather_correlation(repo: Repo) -> list[dict[str, Any]]:
    return q.weather_correlation(repo)


@app.get("/quality", tags=["ops"])
def quality(repo: Repo, limit: Annotated[int, Query(ge=1, le=1000)] = 50) -> list[dict[str, Any]]:
    return q.quality(repo, limit)
