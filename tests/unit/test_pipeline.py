from datetime import date

import pytest
import respx

from gridcarbon import pipeline
from gridcarbon.audit import AuditLog
from gridcarbon.bronze import list_bronze
from gridcarbon.config import Settings
from gridcarbon.domain import Partition
from gridcarbon.http import HttpClient
from gridcarbon.silver.writer import read_silver
from gridcarbon.sources import DAILY_DATASETS, source_for_dataset


def test_extract_then_transform(
    settings: Settings, partition: Partition, client: HttpClient
) -> None:
    records = pipeline.extract(partition, run_id="r1", settings=settings, client=client)
    assert [r.dataset for r in records] == [d for _, d in DAILY_DATASETS]
    assert all(not r.deduplicated for r in records)

    again = pipeline.extract(partition, run_id="r2", settings=settings, client=client)
    assert [r.path for r in again] == [r.path for r in records]  # skipped: bronze already there
    assert len(list_bronze("carbon_intensity", "regional_intensity", partition, settings)) == 1

    report = pipeline.transform(partition, run_id="r3", settings=settings)
    assert report.rows_quarantined == 0
    assert {t["table"]: t["rows_out"] for t in report.tables} == {
        "intensity_national": 48,
        "generation_national": 48 * 9,
        "intensity_regional": 48 * 18,
        "generation_regional": 48 * 18 * 9,
        "weather_hourly": 24 * 14,
    }
    assert read_silver("intensity_regional", [partition], settings)["run_id"].unique().tolist() == [
        "r3"
    ]

    stages = AuditLog(settings).read()
    assert {(s.run_id, s.stage) for s in stages} == {
        ("r1", "extract"),
        ("r2", "extract"),
        ("r3", "transform"),
    }
    assert {s.status for s in stages if s.run_id != "r2"} == {"succeeded"}
    assert {s.status for s in stages if s.run_id == "r2"} == {"skipped"}


def test_transform_without_bronze_is_skipped_not_failed(
    settings: Settings, partition: Partition
) -> None:
    report = pipeline.transform(partition, ["national_intensity"], run_id="r", settings=settings)
    assert report.tables == []
    [stage] = AuditLog(settings).read()
    assert stage.status == "skipped"


def test_unknown_dataset_rejected(settings: Settings, partition: Partition) -> None:
    with pytest.raises(KeyError, match="unknown datasets"):
        pipeline.transform(partition, ["nope"], settings=settings)


def test_source_lookup() -> None:
    assert source_for_dataset("weather_hourly") == "open_meteo"
    with pytest.raises(KeyError):
        source_for_dataset("nope")


def test_backfill_survives_a_failed_partition(
    settings: Settings, api_mock: respx.MockRouter, client: HttpClient
) -> None:
    from tests.conftest import FAILURES

    bad_day = "2026-09-03"
    FAILURES[("regional_intensity", bad_day)] = 500
    parts = [Partition(date(2026, 9, d)) for d in (2, 3, 4)]
    out = pipeline.backfill(
        parts, settings=settings, with_dbt=False, with_serving=False, client=client
    )
    assert out["partitions"] == 2
    assert [f["partition"] for f in out["failed"]] == [bad_day]
    assert "gave up" in out["failed"][0]["error"]


def test_extract_skips_existing_unless_forced(
    settings: Settings, partition: Partition, client: HttpClient, api_mock: respx.MockRouter
) -> None:
    first = pipeline.extract(
        partition, ["national_intensity"], run_id="r1", settings=settings, client=client
    )
    calls_after_first = sum(r.call_count for r in api_mock.routes)
    skipped = pipeline.extract(
        partition, ["national_intensity"], run_id="r2", settings=settings, client=client
    )
    assert skipped[0].path == first[0].path
    assert sum(r.call_count for r in api_mock.routes) == calls_after_first  # no HTTP call
    forced = pipeline.extract(
        partition, ["national_intensity"], run_id="r3", settings=settings, client=client, force=True
    )
    assert forced[0].deduplicated  # same bytes came back, so still one bronze file
    assert sum(r.call_count for r in api_mock.routes) == calls_after_first + 1
    statuses = [(s.run_id, s.status) for s in AuditLog(settings).read()]
    assert statuses == [("r1", "succeeded"), ("r2", "skipped"), ("r3", "succeeded")]
