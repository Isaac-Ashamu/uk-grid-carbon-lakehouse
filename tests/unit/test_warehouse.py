from gridcarbon import pipeline, warehouse
from gridcarbon.config import Settings
from gridcarbon.domain import Partition
from gridcarbon.http import HttpClient


def test_refresh_handles_empty_lake_and_then_data(
    settings: Settings, partition: Partition, client: HttpClient
) -> None:
    counts = warehouse.refresh(settings)
    assert set(counts) >= {"intensity_national", "quarantine", "pipeline_runs"}
    assert all(v == 0 for v in counts.values())
    rels = warehouse.list_relations(settings)
    assert (rels[rels.name == "intensity_national"].type == "BASE TABLE").all()  # placeholder

    pipeline.extract(partition, settings=settings, client=client)
    pipeline.transform(partition, settings=settings)
    counts = warehouse.refresh(settings)  # placeholder tables must be replaced by views
    assert counts["intensity_national"] == 48
    assert counts["weather_hourly"] == 336
    assert counts["pipeline_runs"] == 8
    rels = warehouse.list_relations(settings)
    assert (rels[rels.name == "intensity_national"].type == "VIEW").all()

    frame = warehouse.query(
        "select date, count(*) as n from silver.intensity_regional group by 1", settings=settings
    )
    assert frame.iloc[0]["n"] == 48 * 18
    assert frame.iloc[0]["date"].strftime("%Y-%m-%d") == "2026-09-01"
