from datetime import UTC, date, datetime

import pytest

from gridcarbon.domain import DNO_REGIONS, REGION_BY_ID, REGIONS, Partition, partition_range


def test_partition_window_is_one_utc_day() -> None:
    p = Partition.parse("2026-09-01")
    assert p.start == datetime(2026, 9, 1, tzinfo=UTC)
    assert p.end == datetime(2026, 9, 2, tzinfo=UTC)
    assert p.api_from == "2026-09-01T00:00Z"
    assert p.api_to == "2026-09-02T00:00Z"
    assert str(p) == "2026-09-01"


def test_partition_range_inclusive_and_ordered() -> None:
    parts = partition_range("2026-08-30", date(2026, 9, 2))
    assert [p.key for p in parts] == ["2026-08-30", "2026-08-31", "2026-09-01", "2026-09-02"]
    assert parts[0].next() == parts[1]
    assert parts[1].previous() == parts[0]


def test_partition_range_rejects_reversed() -> None:
    with pytest.raises(ValueError, match="before"):
        partition_range("2026-09-02", "2026-09-01")


def test_regions_are_complete_and_consistent() -> None:
    assert [r.region_id for r in REGIONS] == list(range(1, 19))
    assert len(DNO_REGIONS) == 14
    assert all(r.has_location for r in DNO_REGIONS)
    assert all(not r.has_location and r.is_aggregate for r in REGIONS if r.region_id > 14)
    assert REGION_BY_ID[3].short_name == "North West England"
