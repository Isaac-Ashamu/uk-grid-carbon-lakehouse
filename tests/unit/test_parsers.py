import copy
from datetime import date

import pytest
from hypothesis import given
from hypothesis import settings as h_settings
from hypothesis import strategies as st

from gridcarbon.config import Settings
from gridcarbon.domain import FUELS
from gridcarbon.silver.models import FuelShare
from gridcarbon.silver.parsers import (
    Lineage,
    PartitionQualityError,
    _mix_sums_to_100,
    parse_dataset,
)
from tests.conftest import load_fixture, load_request, shift_payload


def test_national_intensity_trims_the_containing_period(
    settings: Settings, lineage: Lineage
) -> None:
    payload = load_fixture("national_intensity")
    assert len(payload["data"]) == 49  # API returns the half hour containing T00:00Z
    [table] = parse_dataset("national_intensity", payload, lineage, settings)
    assert table.rows_in == 49
    assert table.rows_out == 48
    assert table.rows_quarantined == 0
    assert table.details["trimmed_outside_window"] == 1
    assert table.frame["period_start"].min().isoformat() == "2026-09-01T00:00:00+00:00"
    assert table.frame["period_start"].max().isoformat() == "2026-09-01T23:30:00+00:00"
    assert set(table.frame["bronze_sha256"]) == {"a" * 64}


def test_regional_intensity_explodes_regions_and_fuels(
    settings: Settings, lineage: Lineage
) -> None:
    intensity, generation = parse_dataset(
        "regional_intensity", load_fixture("regional_intensity"), lineage, settings
    )
    assert intensity.rows_out == 48 * 18
    assert generation.rows_out == 48 * 18 * len(FUELS)
    assert sorted(generation.frame["fuel"].unique()) == sorted(FUELS)
    mix_sum = generation.frame.groupby(["period_start", "region_id"])["perc"].sum()
    assert ((mix_sum - 100).abs() <= settings.generation_mix_tolerance_pct).all()


def test_weather_maps_location_id_to_region(settings: Settings, lineage: Lineage) -> None:
    [table] = parse_dataset(
        "weather_hourly",
        load_fixture("weather_hourly"),
        lineage,
        settings,
        load_request("weather_hourly"),
    )
    assert table.rows_out == 24 * 14
    assert sorted(table.frame["region_id"].unique()) == list(range(1, 15))


def test_carbon_factors(settings: Settings, lineage: Lineage) -> None:
    [table] = parse_dataset("carbon_factors", load_fixture("carbon_factors"), lineage, settings)
    assert table.rows_out == 14
    assert table.frame.set_index("fuel_label").loc["Coal", "gco2_per_kwh"] == 937


def test_malformed_record_is_quarantined_not_fatal(settings: Settings, lineage: Lineage) -> None:
    payload = copy.deepcopy(load_fixture("national_intensity"))
    payload["data"][5]["intensity"]["index"] = "banana"
    payload["data"][6]["from"] = "not a timestamp"
    [table] = parse_dataset("national_intensity", payload, lineage, settings)
    assert table.rows_out == 46
    assert table.rows_quarantined == 2
    reasons = table.quarantine["reason"].tolist()
    assert any("index" in r for r in reasons)
    assert any("from" in r for r in reasons)
    assert set(table.quarantine["table"]) == {"intensity_national"}


def test_out_of_range_value_is_quarantined_by_contract(
    settings: Settings, lineage: Lineage
) -> None:
    payload = copy.deepcopy(load_fixture("national_intensity"))
    payload["data"][10]["intensity"]["actual"] = 9999  # structurally fine, semantically impossible
    [table] = parse_dataset("national_intensity", payload, lineage, settings)
    assert table.rows_out == 47
    assert table.rows_quarantined == 1
    assert "actual" in table.quarantine["reason"].iloc[0]
    assert "in_range" in table.quarantine["reason"].iloc[0]


def test_generation_mix_not_summing_to_100_is_quarantined(
    settings: Settings, lineage: Lineage
) -> None:
    payload = copy.deepcopy(load_fixture("national_generation"))
    payload["data"][3]["generationmix"][0]["perc"] = 55.0
    [table] = parse_dataset("national_generation", payload, lineage, settings)
    assert table.rows_quarantined == 1
    assert "sum to 100" in table.quarantine["reason"].iloc[0]
    assert table.frame["period_start"].nunique() == 47


def test_thin_partition_fails_the_run(settings: Settings, lineage: Lineage) -> None:
    payload = load_fixture("national_intensity")
    payload = {"data": payload["data"][:20]}
    with pytest.raises(PartitionQualityError, match="distinct periods"):
        parse_dataset("national_intensity", payload, lineage, settings)


def test_weather_all_null_hours_are_quarantined(settings: Settings, lineage: Lineage) -> None:
    payload = copy.deepcopy(load_fixture("weather_hourly"))
    for var in (
        "temperature_2m",
        "wind_speed_10m",
        "wind_speed_100m",
        "shortwave_radiation",
        "cloud_cover",
        "precipitation",
    ):
        payload[0]["hourly"][var][23] = None
    [table] = parse_dataset(
        "weather_hourly", payload, lineage, settings, load_request("weather_hourly")
    )
    assert table.rows_out == 24 * 14 - 1
    assert table.rows_quarantined == 1
    assert table.details["all_null_hours"] == 1


def test_shifted_fixture_parses_on_another_day(settings: Settings, lineage: Lineage) -> None:
    target = date(2026, 12, 25)
    shifted = shift_payload(load_fixture("regional_intensity"), target)
    lin = Lineage(
        lineage.partition.__class__(target),
        lineage.fetched_at,
        lineage.run_id,
        lineage.bronze_sha256,
    )
    intensity, _ = parse_dataset("regional_intensity", shifted, lin, settings)
    assert intensity.rows_out == 48 * 18
    assert (intensity.frame["period_start"].dt.date == target).all()


def test_unknown_dataset(settings: Settings, lineage: Lineage) -> None:
    with pytest.raises(ValueError, match="no parser"):
        parse_dataset("nope", {}, lineage, settings)


@h_settings(max_examples=200)
@given(st.lists(st.floats(min_value=0, max_value=100, allow_nan=False), min_size=1, max_size=9))
def test_mix_sum_check_matches_tolerance(percs: list[float]) -> None:
    s = Settings(generation_mix_tolerance_pct=2.0)
    mix = [FuelShare(fuel="wind", perc=p) for p in percs]
    assert _mix_sums_to_100(mix, s) == (abs(sum(percs) - 100.0) <= 2.0)
