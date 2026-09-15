from datetime import UTC, datetime
from pathlib import Path

from gridcarbon.bronze import latest_bronze, list_bronze, list_partitions, write_bronze
from gridcarbon.config import Settings
from gridcarbon.domain import Partition
from gridcarbon.http import FetchResult
from gridcarbon.sources.base import RawFetch


def _fetch(partition: Partition, body: bytes, at: datetime) -> RawFetch:
    return RawFetch(
        source="carbon_intensity",
        dataset="national_intensity",
        partition=partition,
        result=FetchResult(
            url="https://x/y",
            status_code=200,
            body=body,
            fetched_at=at,
            elapsed_ms=5,
            attempts=1,
            content_type="application/json",
        ),
        request={"url": "https://x/y"},
    )


def test_write_is_append_only_and_deduplicates(settings: Settings, partition: Partition) -> None:
    t0 = datetime(2026, 9, 2, 6, 0, tzinfo=UTC)
    first = write_bronze(_fetch(partition, b'{"data": [1]}', t0), "run-1", settings)
    same = write_bronze(_fetch(partition, b'{"data": [1]}', t0.replace(hour=7)), "run-2", settings)
    other = write_bronze(_fetch(partition, b'{"data": [2]}', t0.replace(hour=8)), "run-3", settings)

    assert not first.deduplicated
    assert same.deduplicated and same.path == first.path and same.run_id == "run-1"
    assert other.path != first.path

    records = list_bronze("carbon_intensity", "national_intensity", partition, settings)
    assert [r.sha256 for r in records] == [first.sha256, other.sha256]
    assert latest_bronze("carbon_intensity", "national_intensity", partition, settings) == other
    assert other.read_json() == {"data": [2]}
    assert list_partitions("carbon_intensity", "national_intensity", settings) == [partition]


def test_manifest_is_the_commit_marker(settings: Settings, partition: Partition) -> None:
    t0 = datetime(2026, 9, 2, 6, 0, tzinfo=UTC)
    rec = write_bronze(_fetch(partition, b"{}", t0), "run-1", settings)
    manifest = next(Path(rec.path).parent.glob("*.manifest.json"))
    manifest.unlink()  # simulate a crash between data and manifest
    assert list_bronze("carbon_intensity", "national_intensity", partition, settings) == []
    assert list_partitions("carbon_intensity", "national_intensity", settings) == []
