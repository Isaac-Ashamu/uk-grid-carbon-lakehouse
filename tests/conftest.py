"""Shared fixtures.

The recorded fixtures under ``tests/fixtures/2026-09-01`` are real responses
captured from both APIs. ``shift_payload`` re-dates them so a mocked transport
can serve any partition offline, which is what makes a multi-day backfill
testable in CI without network access.
"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from gridcarbon.config import Settings, reset_settings_cache
from gridcarbon.domain import Partition
from gridcarbon.http import HttpClient
from gridcarbon.silver.parsers import Lineage

FIXTURE_DAY = date(2026, 9, 1)
FIXTURE_DIR = Path(__file__).parent / "fixtures" / FIXTURE_DAY.isoformat()
REPO_ROOT = Path(__file__).resolve().parents[1]


def load_fixture(dataset: str) -> Any:
    return json.loads((FIXTURE_DIR / f"{dataset}.json").read_text())


def load_request(dataset: str) -> dict[str, Any]:
    path = FIXTURE_DIR / f"{dataset}.request.json"
    return json.loads(path.read_text()) if path.exists() else {}


_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})(T\d{2}:\d{2}(?::\d{2})?Z?)?$")


def shift_payload(payload: Any, target: date, source: date = FIXTURE_DAY) -> Any:
    """Return a deep copy of ``payload`` with every ISO date moved by (target - source) days."""
    delta = target - source

    def shift(value: Any) -> Any:
        if isinstance(value, str):
            match = _TS_RE.match(value)
            if match:
                day = date.fromisoformat(match.group(1)) + delta
                return day.isoformat() + (match.group(2) or "")
            return value
        if isinstance(value, list):
            return [shift(v) for v in value]
        if isinstance(value, dict):
            return {k: shift(v) for k, v in value.items()}
        return value

    return shift(copy.deepcopy(payload))


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("GRIDCARBON_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("GRIDCARBON_DBT_PROJECT_DIR", str(REPO_ROOT / "dbt"))
    monkeypatch.setenv("GRIDCARBON_HTTP_MIN_BACKOFF_SECONDS", "0")
    monkeypatch.setenv("GRIDCARBON_HTTP_MAX_BACKOFF_SECONDS", "0")
    monkeypatch.setenv("GRIDCARBON_HTTP_MIN_INTERVAL_SECONDS", "0")
    monkeypatch.delenv("GRIDCARBON_POSTGRES_DSN", raising=False)
    reset_settings_cache()
    s = Settings()
    s.ensure_dirs()
    yield s
    reset_settings_cache()


@pytest.fixture
def partition() -> Partition:
    return Partition(FIXTURE_DAY)


@pytest.fixture
def lineage(partition: Partition) -> Lineage:
    return Lineage(
        partition=partition,
        fetched_at=datetime(2026, 9, 2, 6, 0, tzinfo=UTC),
        run_id="test-run",
        bronze_sha256="a" * 64,
    )


# (dataset, day) pairs a test wants the mocked API to fail for, with the status to return.
FAILURES: dict[tuple[str, str], int] = {}


def _route_all(router: respx.MockRouter, settings: Settings) -> None:
    base = settings.carbon_intensity_base_url

    def carbon(dataset: str, path_re: str) -> None:
        def responder(request: httpx.Request) -> httpx.Response:
            m = re.search(r"(\d{4}-\d{2}-\d{2})T00:00Z/", str(request.url))
            target = date.fromisoformat(m.group(1)) if m else FIXTURE_DAY
            if (dataset, target.isoformat()) in FAILURES:
                return httpx.Response(FAILURES[(dataset, target.isoformat())])
            return httpx.Response(200, json=shift_payload(load_fixture(dataset), target))

        router.get(url__regex=rf"{re.escape(base)}{path_re}").mock(side_effect=responder)

    carbon("national_intensity", r"/intensity/\d{4}-\d{2}-\d{2}T00:00Z/.*")
    carbon("national_generation", r"/generation/.*")
    carbon("regional_intensity", r"/regional/intensity/.*")
    router.get(f"{base}/intensity/factors").mock(
        return_value=httpx.Response(200, json=load_fixture("carbon_factors"))
    )

    def meteo(request: httpx.Request) -> httpx.Response:
        target = date.fromisoformat(request.url.params["start_date"])
        return httpx.Response(200, json=shift_payload(load_fixture("weather_hourly"), target))

    router.get(url__regex=rf"{re.escape(settings.open_meteo_base_url)}.*").mock(side_effect=meteo)


@pytest.fixture
def api_mock(settings: Settings) -> Iterator[respx.MockRouter]:
    FAILURES.clear()
    with respx.mock(assert_all_called=False) as router:
        _route_all(router, settings)
        yield router
    FAILURES.clear()


@pytest.fixture
def client(settings: Settings, api_mock: respx.MockRouter) -> Iterator[HttpClient]:
    with HttpClient(settings, sleeper=lambda _s: None) as c:
        yield c


def days(start: date, n: int) -> list[Partition]:
    return [Partition(start + timedelta(days=i)) for i in range(n)]
