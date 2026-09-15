"""``gridcarbon`` command line.

gridcarbon extract 2026-09-01                # bronze
gridcarbon transform 2026-09-01              # silver (+ quarantine)
gridcarbon build                             # warehouse views + dbt build
gridcarbon load                              # Postgres
gridcarbon run 2026-09-01                    # all of the above for one day
gridcarbon backfill 2026-08-01 2026-08-31    # many days, then build + load once
gridcarbon reference                         # carbon factors snapshot
gridcarbon status                            # what is in the lake
gridcarbon runs --last 20                    # audit trail
gridcarbon query "select ..."                # ad hoc SQL against the warehouse
gridcarbon serve                             # FastAPI on :8000
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.table import Table

from gridcarbon import __version__, pipeline
from gridcarbon.audit import AuditLog, new_run_id
from gridcarbon.bronze import list_partitions
from gridcarbon.config import get_settings
from gridcarbon.domain import Partition, partition_range
from gridcarbon.logging import configure_logging
from gridcarbon.silver.writer import list_silver_partitions
from gridcarbon.sources import ALL_DATASETS, DAILY_DATASETS

app = typer.Typer(
    help="Medallion lakehouse over GB grid carbon intensity and weather.",
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode="rich",
)
console = Console()

DateArg = Annotated[str, typer.Argument(help="UTC day, YYYY-MM-DD")]
DatasetsOpt = Annotated[
    list[str] | None,
    typer.Option("--dataset", "-d", help="Limit to these datasets (repeatable)."),
]


@app.callback()
def _main(
    log_level: Annotated[str, typer.Option(envvar="GRIDCARBON_LOG_LEVEL")] = "INFO",
) -> None:
    configure_logging(log_level)


def _yesterday() -> str:
    return (datetime.now(tz=UTC).date() - timedelta(days=1)).isoformat()


def _emit(payload: Any) -> None:
    console.print_json(json.dumps(payload, default=str))


@app.command()
def version() -> None:
    """Print the package version."""
    console.print(__version__)


ForceOpt = Annotated[
    bool, typer.Option("--force", help="Re-fetch even if bronze already has the partition.")
]


@app.command()
def extract(
    day: DateArg = "yesterday", datasets: DatasetsOpt = None, force: ForceOpt = False
) -> None:
    """Fetch the raw API responses for a day into bronze (skips days already fetched)."""
    partition = Partition.parse(_yesterday() if day == "yesterday" else day)
    records = pipeline.extract(partition, datasets, force=force)
    _emit(
        [
            {"dataset": r.dataset, "bytes": r.bytes, "dedup": r.deduplicated, "path": r.path}
            for r in records
        ]
    )


@app.command()
def transform(day: DateArg = "yesterday", datasets: DatasetsOpt = None) -> None:
    """Parse, validate and publish a day from bronze into silver."""
    partition = Partition.parse(_yesterday() if day == "yesterday" else day)
    report = pipeline.transform(partition, datasets)
    _emit(
        {
            "partition": report.partition,
            "rows_out": report.rows_out,
            "rows_quarantined": report.rows_quarantined,
            "tables": report.tables,
        }
    )


@app.command()
def reference() -> None:
    """Snapshot the slowly changing reference data (emission factors)."""
    run_id = new_run_id()
    pipeline.extract_reference(run_id)
    today = Partition(datetime.now(tz=UTC).date())
    report = pipeline.transform(today, ["carbon_factors"], run_id, include_reference=True)
    _emit({"partition": report.partition, "tables": report.tables})


@app.command()
def build(
    select: Annotated[str | None, typer.Option(help="dbt --select expression")] = None,
) -> None:
    """Refresh the DuckDB views over silver and run `dbt build` (seeds, models, tests)."""
    out = pipeline.build_marts(select=select)
    _emit({k: v for k, v in out.items() if k != "stdout_tail"})


@app.command()
def load() -> None:
    """Publish the marts to Postgres (needs GRIDCARBON_POSTGRES_DSN)."""
    _emit(pipeline.load_serving())


@app.command()
def run(
    day: DateArg = "yesterday",
    no_dbt: Annotated[bool, typer.Option("--no-dbt")] = False,
    no_load: Annotated[bool, typer.Option("--no-load")] = False,
    force: ForceOpt = False,
) -> None:
    """Extract, transform, build and load one day."""
    partition = Partition.parse(_yesterday() if day == "yesterday" else day)
    out = pipeline.run_partition(
        partition, with_dbt=not no_dbt, with_serving=not no_load, force=force
    )
    if "dbt" in out:
        out["dbt"].pop("stdout_tail", None)
    _emit(out)


@app.command()
def backfill(
    start: Annotated[str, typer.Argument(help="first UTC day, inclusive")],
    end: Annotated[str, typer.Argument(help="last UTC day, inclusive")] = "yesterday",
    no_dbt: Annotated[bool, typer.Option("--no-dbt")] = False,
    no_load: Annotated[bool, typer.Option("--no-load")] = False,
    force: ForceOpt = False,
) -> None:
    """Extract and transform a date range, then build and load once. Resumable."""
    partitions = partition_range(start, _yesterday() if end == "yesterday" else end)
    console.print(f"backfilling {len(partitions)} partitions: {partitions[0]} .. {partitions[-1]}")
    out = pipeline.backfill(partitions, with_dbt=not no_dbt, with_serving=not no_load, force=force)
    if "dbt" in out:
        out["dbt"].pop("stdout_tail", None)
    _emit(out)


@app.command()
def status() -> None:
    """Show what is in bronze and silver, per dataset."""
    settings = get_settings()
    table = Table(title=f"lake at {settings.data_root.resolve()}")
    table.add_column("source")
    table.add_column("dataset")
    table.add_column("bronze days", justify="right")
    table.add_column("bronze range")
    for source, dataset in ALL_DATASETS:
        parts = list_partitions(source, dataset, settings)
        rng = f"{parts[0]} .. {parts[-1]}" if parts else "-"
        table.add_row(source, dataset, str(len(parts)), rng)
    console.print(table)

    from gridcarbon.silver.schemas import SCHEMAS

    table = Table(title="silver")
    table.add_column("table")
    table.add_column("days", justify="right")
    table.add_column("range")
    for name in SCHEMAS:
        parts = list_silver_partitions(name, settings)
        rng = f"{parts[0]} .. {parts[-1]}" if parts else "-"
        table.add_row(name, str(len(parts)), rng)
    console.print(table)


@app.command()
def runs(last: Annotated[int, typer.Option(help="how many stage runs to show")] = 20) -> None:
    """Show the most recent stage runs from the audit log."""
    entries = AuditLog().read()[-last:]
    table = Table(title="pipeline runs")
    for col in (
        "started_at",
        "run_id",
        "stage",
        "dataset",
        "partition",
        "status",
        "ms",
        "in",
        "out",
        "quarantined",
        "error",
    ):
        table.add_column(
            col, justify="right" if col in {"ms", "in", "out", "quarantined"} else "left"
        )
    for e in entries:
        style = {"succeeded": "green", "failed": "red", "skipped": "yellow"}.get(e.status, "")
        table.add_row(
            e.started_at[:19],
            e.run_id,
            e.stage,
            e.dataset or "",
            e.partition or "",
            f"[{style}]{e.status}[/{style}]" if style else e.status,
            str(e.duration_ms or ""),
            str(e.rows_in or ""),
            str(e.rows_out or ""),
            str(e.rows_quarantined or ""),
            (e.error or "")[:60],
        )
    console.print(table)


@app.command()
def query(
    sql: Annotated[str, typer.Argument(help="SQL to run against the DuckDB warehouse")],
    limit: int = 50,
) -> None:
    """Run ad hoc SQL against the warehouse and print the result."""
    from gridcarbon import warehouse

    frame = warehouse.query(sql).head(limit)
    table = Table(show_lines=False)
    for col in frame.columns:
        table.add_column(str(col))
    for row in frame.itertuples(index=False):
        table.add_row(*[str(v) for v in row])
    console.print(table)


@app.command()
def daily_datasets() -> None:
    """List the datasets the daily pipeline produces."""
    _emit([{"source": s, "dataset": d} for s, d in DAILY_DATASETS])


@app.command()
def serve(host: str = "0.0.0.0", port: int = 8000, reload: bool = False) -> None:
    """Start the read API."""
    import uvicorn

    uvicorn.run("gridcarbon.api.app:app", host=host, port=port, reload=reload)


if __name__ == "__main__":
    app()
