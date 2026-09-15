"""The pipeline stages, as plain functions.

Airflow, the CLI and the tests all call these. Nothing in here knows about
schedulers, so the whole pipeline can be exercised in a unit test with a
temporary ``data_root`` and recorded fixtures.

    extract(partition)   bronze   <- APIs
    transform(partition) silver   <- bronze (latest fetch per dataset)
    refresh_warehouse()  duckdb   <- silver views
    build_marts()        marts    <- dbt build
    load_serving()       postgres <- marts
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from gridcarbon import warehouse
from gridcarbon.audit import AuditLog, new_run_id
from gridcarbon.bronze import BronzeRecord, latest_bronze, write_bronze
from gridcarbon.config import Settings, get_settings
from gridcarbon.domain import Partition
from gridcarbon.http import HttpClient
from gridcarbon.logging import bind_run_context, clear_run_context, get_logger
from gridcarbon.silver.parsers import Lineage, ParsedTable, parse_dataset
from gridcarbon.silver.writer import write_quarantine, write_silver_partition
from gridcarbon.sources import ALL_DATASETS, DAILY_DATASETS, REFERENCE_DATASETS, build_sources

log = get_logger(__name__)


class DbtError(RuntimeError):
    pass


@dataclass
class TransformReport:
    partition: str
    tables: list[dict[str, Any]] = field(default_factory=list)

    @property
    def rows_out(self) -> int:
        return sum(int(t["rows_out"]) for t in self.tables)

    @property
    def rows_quarantined(self) -> int:
        return sum(int(t["rows_quarantined"]) for t in self.tables)


def _select(
    datasets: Sequence[str] | None, universe: Iterable[tuple[str, str]]
) -> list[tuple[str, str]]:
    pairs = list(universe)
    if datasets is None:
        return pairs
    wanted = set(datasets)
    chosen = [p for p in pairs if p[1] in wanted]
    missing = wanted - {p[1] for p in chosen}
    if missing:
        raise KeyError(f"unknown datasets: {sorted(missing)}")
    return chosen


# --------------------------------------------------------------------------
# extract
# --------------------------------------------------------------------------


def extract(
    partition: Partition,
    datasets: Sequence[str] | None = None,
    run_id: str | None = None,
    settings: Settings | None = None,
    client: HttpClient | None = None,
    include_reference: bool = False,
    force: bool = False,
) -> list[BronzeRecord]:
    """Fetch each dataset for the partition into bronze.

    A dataset that already has a committed bronze file for the partition is
    skipped unless ``force`` is set, which makes an interrupted backfill
    resumable without re-hitting the APIs. ``force`` re-fetches and, if the
    response changed, appends a new bronze file (silver takes the latest).
    """
    settings = settings or get_settings()
    settings.ensure_dirs()
    run_id = run_id or new_run_id()
    universe = ALL_DATASETS if include_reference else DAILY_DATASETS
    pairs = _select(datasets, universe)
    sources = build_sources(settings)
    audit = AuditLog(settings)
    records: list[BronzeRecord] = []
    bind_run_context(run_id=run_id, partition=partition.key)
    owned_client = client is None
    client = client or HttpClient(settings)
    try:
        for source_name, dataset in pairs:
            with audit.stage(run_id, "extract", dataset, partition.key) as stage:
                existing = (
                    None if force else latest_bronze(source_name, dataset, partition, settings)
                )
                if existing is not None:
                    stage.status = "skipped"
                    stage.error = "bronze already present; use force to re-fetch"
                    stage.set(rows_out=0, path=existing.path, bronze_sha256=existing.sha256)
                    log.info("extract.skipped_existing", dataset=dataset, partition=partition.key)
                    records.append(existing)
                    continue
                fetched = 0
                for raw in sources[source_name].fetch(client, partition, dataset):
                    record = write_bronze(raw, run_id, settings)
                    records.append(record)
                    fetched += 1
                    stage.set(bytes=record.bytes, deduplicated=record.deduplicated, url=record.url)
                stage.set(rows_out=fetched)
    finally:
        if owned_client:
            client.close()
        clear_run_context()
    return records


def extract_reference(
    run_id: str | None = None, settings: Settings | None = None, client: HttpClient | None = None
) -> list[BronzeRecord]:
    """Reference datasets are snapshotted under today's partition."""
    from datetime import UTC

    today = Partition(datetime.now(tz=UTC).date())
    return extract(
        today,
        [d for _, d in REFERENCE_DATASETS],
        run_id,
        settings,
        client,
        include_reference=True,
    )


# --------------------------------------------------------------------------
# transform
# --------------------------------------------------------------------------


def transform(
    partition: Partition,
    datasets: Sequence[str] | None = None,
    run_id: str | None = None,
    settings: Settings | None = None,
    include_reference: bool = False,
) -> TransformReport:
    settings = settings or get_settings()
    settings.ensure_dirs()
    run_id = run_id or new_run_id()
    universe = ALL_DATASETS if include_reference else DAILY_DATASETS
    pairs = _select(datasets, universe)
    audit = AuditLog(settings)
    report = TransformReport(partition.key)
    bind_run_context(run_id=run_id, partition=partition.key)
    try:
        for source_name, dataset in pairs:
            with audit.stage(run_id, "transform", dataset, partition.key) as stage:
                record = latest_bronze(source_name, dataset, partition, settings)
                if record is None:
                    stage.status = "skipped"
                    stage.error = "no bronze data for partition"
                    log.warning("transform.no_bronze", dataset=dataset, partition=partition.key)
                    continue
                lineage = Lineage(
                    partition=partition,
                    fetched_at=record.fetched_at_dt,
                    run_id=run_id,
                    bronze_sha256=record.sha256,
                )
                tables = parse_dataset(
                    dataset, record.read_json(), lineage, settings, record.request
                )
                _publish(tables, partition, settings, report)
                stage.set(
                    rows_in=sum(t.rows_in for t in tables),
                    rows_out=sum(t.rows_out for t in tables),
                    rows_quarantined=sum(t.rows_quarantined for t in tables),
                    tables=[t.name for t in tables],
                    bronze_sha256=record.sha256,
                    bronze_fetched_at=record.fetched_at,
                    **{f"{t.name}.{k}": v for t in tables for k, v in t.details.items()},
                )
    finally:
        clear_run_context()
    return report


def _publish(
    tables: list[ParsedTable], partition: Partition, settings: Settings, report: TransformReport
) -> None:
    for table in tables:
        path = write_silver_partition(table, partition, settings)
        qpath = write_quarantine(table, partition, settings)
        report.tables.append(
            {
                "table": table.name,
                "rows_in": table.rows_in,
                "rows_out": table.rows_out,
                "rows_quarantined": table.rows_quarantined,
                "path": str(path),
                "quarantine_path": str(qpath) if qpath else None,
                **table.details,
            }
        )


# --------------------------------------------------------------------------
# warehouse + dbt
# --------------------------------------------------------------------------


def refresh_warehouse(settings: Settings | None = None) -> dict[str, int]:
    return warehouse.refresh(settings)


def _dbt_command() -> list[str]:
    exe = shutil.which("dbt")
    if exe:
        return [exe]
    return [sys.executable, "-m", "dbt.cli.main"]


def run_dbt(
    args: Sequence[str],
    settings: Settings | None = None,
    run_id: str | None = None,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a dbt subcommand against the project, pointed at this warehouse."""
    settings = settings or get_settings()
    settings.ensure_dirs()
    project_dir = Path(settings.dbt_project_dir).resolve()
    env = {
        **os.environ,
        "GRIDCARBON_DUCKDB_PATH": str(settings.warehouse_path.resolve()),
        "DBT_PROFILES_DIR": str(project_dir),
        "DBT_PROJECT_DIR": str(project_dir),
        **(extra_env or {}),
    }
    cmd = [
        *_dbt_command(),
        *args,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
        "--target",
        settings.dbt_target,
    ]
    log.info("dbt.run", cmd=" ".join(cmd[-8:]))
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    tail = "\n".join(proc.stdout.strip().splitlines()[-25:])
    if proc.returncode != 0:
        log.error(
            "dbt.failed", returncode=proc.returncode, stdout_tail=tail, stderr=proc.stderr[-2000:]
        )
        raise DbtError(
            f"dbt {' '.join(args)} failed with exit code {proc.returncode}:\n"
            f"{tail}\n{proc.stderr[-2000:]}"
        )
    log.info("dbt.finished", stdout_tail=tail)
    return proc


def build_marts(
    settings: Settings | None = None, run_id: str | None = None, select: str | None = None
) -> dict[str, Any]:
    """Refresh the warehouse views, then ``dbt build`` (seeds, models and tests)."""
    settings = settings or get_settings()
    run_id = run_id or new_run_id()
    audit = AuditLog(settings)
    with audit.stage(run_id, "dbt_build", None, None) as stage:
        counts = refresh_warehouse(settings)
        args = ["build"]
        if select:
            args += ["--select", select]
        proc = run_dbt(args, settings, run_id)
        summary = _dbt_run_results(settings)
        stage.set(rows_in=sum(counts.values()), **summary)
        return {"warehouse": counts, **summary, "stdout_tail": proc.stdout[-1500:]}


def _dbt_run_results(settings: Settings) -> dict[str, Any]:
    path = Path(settings.dbt_project_dir) / "target" / "run_results.json"
    if not path.exists():
        return {}
    results = json.loads(path.read_text())
    statuses: dict[str, int] = {}
    for result in results.get("results", []):
        statuses[result["status"]] = statuses.get(result["status"], 0) + 1
    return {"dbt_results": statuses, "dbt_elapsed_s": round(results.get("elapsed_time", 0.0), 1)}


# --------------------------------------------------------------------------
# serving
# --------------------------------------------------------------------------


def load_serving(settings: Settings | None = None, run_id: str | None = None) -> dict[str, int]:
    from gridcarbon.loaders.postgres import load_marts

    settings = settings or get_settings()
    run_id = run_id or new_run_id()
    audit = AuditLog(settings)
    with audit.stage(run_id, "load_postgres", None, None) as stage:
        if not settings.postgres_dsn:
            stage.status = "skipped"
            stage.error = "GRIDCARBON_POSTGRES_DSN not set"
            log.warning("load.skipped", reason="no postgres dsn")
            return {}
        counts = load_marts(settings)
        stage.set(rows_out=sum(counts.values()), tables=sorted(counts))
        return counts


# --------------------------------------------------------------------------
# end to end
# --------------------------------------------------------------------------


def run_partition(
    partition: Partition,
    settings: Settings | None = None,
    run_id: str | None = None,
    with_dbt: bool = True,
    with_serving: bool = True,
    client: HttpClient | None = None,
    force: bool = False,
) -> dict[str, Any]:
    settings = settings or get_settings()
    run_id = run_id or new_run_id()
    extract(partition, run_id=run_id, settings=settings, client=client, force=force)
    report = transform(partition, run_id=run_id, settings=settings)
    out: dict[str, Any] = {
        "run_id": run_id,
        "partition": partition.key,
        "rows_out": report.rows_out,
        "rows_quarantined": report.rows_quarantined,
    }
    if with_dbt:
        out["dbt"] = build_marts(settings, run_id)
    if with_serving:
        out["serving"] = load_serving(settings, run_id)
    return out


def backfill(
    partitions: Sequence[Partition],
    settings: Settings | None = None,
    run_id: str | None = None,
    with_dbt: bool = True,
    with_serving: bool = True,
    client: HttpClient | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Extract and transform many partitions, then build the marts once."""
    settings = settings or get_settings()
    run_id = run_id or new_run_id()
    totals: dict[str, Any] = {"partitions": 0, "rows_out": 0, "rows_quarantined": 0, "failed": []}
    owned = client is None
    client = client or HttpClient(settings)
    try:
        for partition in partitions:
            try:
                extract(partition, run_id=run_id, settings=settings, client=client, force=force)
                report = transform(partition, run_id=run_id, settings=settings)
            except Exception as exc:  # keep going; the audit log has the traceback
                log.error("backfill.partition_failed", partition=partition.key, error=str(exc))
                totals["failed"].append({"partition": partition.key, "error": str(exc)})
                continue
            totals["partitions"] += 1
            totals["rows_out"] += report.rows_out
            totals["rows_quarantined"] += report.rows_quarantined
    finally:
        if owned:
            client.close()
    out: dict[str, Any] = {"run_id": run_id, **totals}
    if with_dbt:
        out["dbt"] = build_marts(settings, run_id)
    if with_serving:
        out["serving"] = load_serving(settings, run_id)
    return out
