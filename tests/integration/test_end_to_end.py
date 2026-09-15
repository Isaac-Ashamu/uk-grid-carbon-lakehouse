"""Whole pipeline on recorded fixtures: five days, bronze -> silver -> dbt -> API (-> Postgres)."""

from __future__ import annotations

import os
from datetime import date

import pytest
from fastapi.testclient import TestClient

from gridcarbon import pipeline, warehouse
from gridcarbon.config import Settings, reset_settings_cache
from gridcarbon.domain import Partition
from gridcarbon.http import HttpClient
from tests.conftest import days

pytestmark = pytest.mark.integration

START = date(2026, 9, 1)
N_DAYS = 5


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Settings:
    """Backfill and build once for the whole module (dbt takes a few seconds)."""
    import respx

    from tests.conftest import REPO_ROOT, _route_all

    root = tmp_path_factory.mktemp("lake")
    os.environ["GRIDCARBON_DATA_ROOT"] = str(root)
    os.environ["GRIDCARBON_DBT_PROJECT_DIR"] = str(REPO_ROOT / "dbt")
    os.environ["GRIDCARBON_HTTP_MIN_BACKOFF_SECONDS"] = "0"
    os.environ.pop("GRIDCARBON_API_BACKEND", None)
    reset_settings_cache()
    settings = Settings()
    settings.ensure_dirs()
    with respx.mock(assert_all_called=False) as router:
        _route_all(router, settings)
        with HttpClient(settings, sleeper=lambda _s: None) as client:
            [factors] = pipeline.extract_reference(settings=settings, client=client)
            pipeline.transform(
                Partition.parse(factors.partition),
                ["carbon_factors"],
                settings=settings,
                include_reference=True,
            )
            out = pipeline.backfill(
                days(START, N_DAYS),
                settings=settings,
                with_dbt=True,
                with_serving=False,
                client=client,
            )
    assert out["failed"] == []
    assert out["partitions"] == N_DAYS
    assert out["dbt"]["dbt_results"] == {"success": 22, "pass": 61}, out["dbt"]["dbt_results"]
    yield settings
    reset_settings_cache()


def test_marts_have_expected_grain(built: Settings) -> None:
    q = lambda sql: warehouse.query(sql, settings=built)  # noqa: E731
    assert q("select count(*) n from marts.fct_intensity_national_halfhourly").n[0] == 48 * N_DAYS
    assert (
        q("select count(*) n from marts.fct_intensity_regional_halfhourly").n[0] == 48 * 18 * N_DAYS
    )
    assert q("select count(*) n from marts.agg_daily_national where is_complete_day").n[0] == N_DAYS
    assert q("select count(*) n from marts.dim_date").n[0] == N_DAYS
    assert q("select count(*) n from marts.agg_weather_intensity_correlation").n[0] == 14
    assert (
        q(
            "select count(distinct weather_method) n "
            "from marts.fct_intensity_regional_halfhourly where not is_aggregate"
        ).n[0]
        >= 2
    )
    dq = q("select dataset, transform_status, national_day_complete from marts.data_quality_daily")
    assert set(dq.transform_status) == {"succeeded"}
    assert set(dq.dataset) == {
        "national_intensity",
        "national_generation",
        "regional_intensity",
        "weather_hourly",
        "carbon_factors",
    }
    assert dq[dq.dataset != "carbon_factors"].national_day_complete.all()
    assert dq[dq.dataset == "carbon_factors"].national_day_complete.isna().all()
    factors = q("select fuel, emission_factor_gco2_kwh from marts.dim_fuel_factors")
    assert factors.set_index("fuel").loc["coal", "emission_factor_gco2_kwh"] == 937


def test_rebuild_is_idempotent(built: Settings) -> None:
    before = warehouse.query(
        "select count(*) n from marts.fct_intensity_regional_halfhourly", settings=built
    ).n[0]
    out = pipeline.build_marts(built)
    assert out["dbt_results"] == {"success": 22, "pass": 61}
    after = warehouse.query(
        "select count(*) n from marts.fct_intensity_regional_halfhourly", settings=built
    ).n[0]
    assert before == after


def test_api_serves_the_marts(built: Settings) -> None:
    from gridcarbon.api.app import app

    client = TestClient(app)
    health = client.get("/health").json()
    assert health["status"] == "ok" and health["backend"] == "duckdb"
    assert health["days"] == N_DAYS and health["complete_days"] == N_DAYS
    assert len(client.get("/regions").json()) == 18
    assert len(client.get("/intensity/national", params={"day": "2026-09-03"}).json()) == 48
    regional = client.get("/intensity/regional/3", params={"day": "2026-09-03"}).json()
    assert len(regional) == 48 and regional[0]["region_name"] == "North West England"
    assert regional[0]["wind_speed_100m_ms"] is not None
    assert (
        client.get("/daily/national", params={"start": "2026-09-01", "end": "2026-09-05"}).json()[
            -1
        ]["date_day"]
        == "2026-09-05"
    )
    hours = client.get("/regions/13/cleanest-hours").json()
    assert [h["cleanliness_rank"] for h in hours] == list(range(1, 25))
    assert client.get("/regions/99/cleanest-hours").status_code == 404
    assert (
        client.get(
            "/daily/national", params={"start": "2026-09-05", "end": "2026-09-01"}
        ).status_code
        == 422
    )
    assert (
        len(client.get("/quality", params={"limit": 200}).json()) == 4 * N_DAYS + 1
    )  # + the factors snapshot


@pytest.mark.postgres
def test_postgres_load_and_api(built: Settings) -> None:
    dsn = os.environ.get("GRIDCARBON_POSTGRES_DSN")
    if not dsn:
        pytest.skip("GRIDCARBON_POSTGRES_DSN not set")
    import uuid

    import psycopg

    from gridcarbon.loaders.postgres import MART_TABLES, load_marts, table_counts

    schema = f"test_marts_{uuid.uuid4().hex[:8]}"  # isolated from any real load in the same DB
    settings = Settings(
        postgres_dsn=dsn,
        postgres_schema=schema,
        data_root=built.data_root,
        dbt_project_dir=built.dbt_project_dir,
    )
    try:
        counts = load_marts(settings)
        assert set(counts) == {m.name for m in MART_TABLES}
        assert counts["fct_intensity_regional_halfhourly"] == 48 * 18 * N_DAYS
        again = load_marts(settings)  # upserts and swaps are idempotent
        assert table_counts(settings) == counts == again

        os.environ["GRIDCARBON_POSTGRES_DSN"] = dsn
        os.environ["GRIDCARBON_POSTGRES_SCHEMA"] = schema
        os.environ["GRIDCARBON_API_BACKEND"] = "postgres"
        reset_settings_cache()
        try:
            from gridcarbon.api.app import app

            client = TestClient(app)
            assert client.get("/health").json()["backend"] == "postgres"
            assert (
                len(client.get("/intensity/regional/13", params={"day": "2026-09-02"}).json()) == 48
            )
        finally:
            for var in (
                "GRIDCARBON_API_BACKEND",
                "GRIDCARBON_POSTGRES_DSN",
                "GRIDCARBON_POSTGRES_SCHEMA",
            ):
                os.environ.pop(var, None)
            reset_settings_cache()
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
