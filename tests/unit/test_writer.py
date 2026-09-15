from gridcarbon.config import Settings
from gridcarbon.domain import Partition
from gridcarbon.silver.parsers import Lineage, parse_dataset
from gridcarbon.silver.writer import (
    list_silver_partitions,
    read_silver,
    write_quarantine,
    write_silver_partition,
)
from tests.conftest import load_fixture


def test_partition_overwrite_and_quarantine_lifecycle(
    settings: Settings, partition: Partition, lineage: Lineage
) -> None:
    payload = load_fixture("national_intensity")
    [clean] = parse_dataset("national_intensity", payload, lineage, settings)
    write_silver_partition(clean, partition, settings)
    assert write_quarantine(clean, partition, settings) is None
    assert len(read_silver("intensity_national", [partition], settings)) == 48

    bad = {
        "data": [
            dict(p, intensity=dict(p["intensity"], index="???")) if 1 <= i <= 3 else p
            for i, p in enumerate(payload["data"])
        ]
    }
    [dirty] = parse_dataset("national_intensity", bad, lineage, settings)
    write_silver_partition(dirty, partition, settings)
    qpath = write_quarantine(dirty, partition, settings)
    assert qpath is not None and qpath.exists()
    assert (
        len(read_silver("intensity_national", [partition], settings)) == 45
    )  # replaced, not appended
    assert len(read_silver("quarantine", settings=settings)) == 3

    write_silver_partition(clean, partition, settings)
    assert write_quarantine(clean, partition, settings) is None
    assert not qpath.exists()  # a clean re-run clears the day's rejects
    assert len(read_silver("intensity_national", settings=settings)) == 48
    assert list_silver_partitions("intensity_national", settings) == [partition]


def test_read_missing_table_returns_empty_frame_with_schema(settings: Settings) -> None:
    frame = read_silver("weather_hourly", settings=settings)
    assert frame.empty
    assert "wind_speed_100m" in frame.columns
