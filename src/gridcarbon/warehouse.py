"""DuckDB warehouse over the silver lake.

The warehouse is a single DuckDB file. ``refresh`` (re)creates:

* ``silver.<table>``  views over the Hive-partitioned Parquet of each silver table
* ``silver.quarantine`` view over every quarantine file
* ``audit.pipeline_runs`` view over the JSONL run log

dbt then builds ``staging`` and ``marts`` on top of those views. Because the
views read the Parquet lazily, refreshing the warehouse after a backfill is
instant and there is exactly one copy of the silver data on disk.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import duckdb
import pandas as pd

from gridcarbon.config import Settings, get_settings
from gridcarbon.logging import get_logger
from gridcarbon.silver.schemas import SCHEMAS
from gridcarbon.silver.writer import table_dir

log = get_logger(__name__)

# pandera dtype string -> DuckDB type, for empty placeholder tables.
_DUCK_TYPES = {
    "datetime64[ns, UTC]": "TIMESTAMP WITH TIME ZONE",
    "datetime64[ns]": "TIMESTAMP",
    "Int64": "BIGINT",
    "int64": "BIGINT",
    "float64": "DOUBLE",
    "str": "VARCHAR",
}


@contextmanager
def connect(
    settings: Settings | None = None, read_only: bool = False
) -> Iterator[duckdb.DuckDBPyConnection]:
    settings = settings or get_settings()
    settings.warehouse_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.warehouse_path), read_only=read_only)
    # Settlement periods are UTC. Never let the host's local zone shift a day boundary.
    con.execute("SET TimeZone = 'UTC'")
    try:
        yield con
    finally:
        con.close()


def _placeholder_ddl(table: str) -> str:
    cols = []
    for name, column in SCHEMAS[table].columns.items():
        dtype = str(column.dtype)
        duck = _DUCK_TYPES.get(dtype, "VARCHAR")
        cols.append(f'"{name}" {duck}')
    cols.append('"date" DATE')
    return ", ".join(cols)


def _count(con: duckdb.DuckDBPyConnection, relation: str) -> int:
    row = con.execute(f"SELECT count(*) FROM {relation}").fetchone()
    return int(row[0]) if row else 0


def _drop_relation(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> None:
    """DROP whatever exists under this name; DuckDB refuses DROP VIEW on a table and vice versa."""
    row = con.execute(
        "SELECT table_type FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, name],
    ).fetchone()
    if row is None:
        return
    kind = "VIEW" if row[0] == "VIEW" else "TABLE"
    con.execute(f"DROP {kind} IF EXISTS {schema}.{name}")


def refresh(settings: Settings | None = None) -> dict[str, int]:
    """Point the warehouse views at whatever is in the lake right now."""
    settings = settings or get_settings()
    counts: dict[str, int] = {}
    with connect(settings) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute("CREATE SCHEMA IF NOT EXISTS audit")
        for table in SCHEMAS:
            root = table_dir(settings, table)
            pattern = str(root / "date=*" / "*.parquet")
            has_files = root.exists() and any(root.glob("date=*/*.parquet"))
            _drop_relation(con, "silver", table)
            if has_files:
                con.execute(
                    f"CREATE VIEW silver.{table} AS "
                    f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true, "
                    f"hive_types = {{'date': DATE}}, union_by_name = true)"
                )
                counts[table] = _count(con, f"silver.{table}")
            else:
                con.execute(f"CREATE TABLE silver.{table} ({_placeholder_ddl(table)})")
                counts[table] = 0

        runs_path = settings.audit_dir / "runs.jsonl"
        _drop_relation(con, "audit", "pipeline_runs")
        if runs_path.exists() and runs_path.stat().st_size > 0:
            con.execute(
                "CREATE VIEW audit.pipeline_runs AS "
                f"SELECT * EXCLUDE (details), to_json(details) AS details "
                f"FROM read_json_auto('{runs_path}', format = 'newline_delimited')"
            )
            counts["pipeline_runs"] = _count(con, "audit.pipeline_runs")
        else:
            con.execute(
                "CREATE TABLE audit.pipeline_runs (run_id VARCHAR, stage VARCHAR, dataset VARCHAR, "
                "partition VARCHAR, started_at VARCHAR, finished_at VARCHAR, status VARCHAR, "
                "duration_ms BIGINT, rows_in BIGINT, rows_out BIGINT, rows_quarantined BIGINT, "
                "error VARCHAR, details VARCHAR)"
            )
            counts["pipeline_runs"] = 0
    log.info("warehouse.refreshed", **counts)
    return counts


def query(
    sql: str, params: list[Any] | None = None, settings: Settings | None = None
) -> pd.DataFrame:
    with connect(settings, read_only=True) as con:
        return con.execute(sql, params or []).df()


def list_relations(settings: Settings | None = None) -> pd.DataFrame:
    return query(
        "SELECT table_schema AS schema, table_name AS name, table_type AS type "
        "FROM information_schema.tables "
        "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') "
        "ORDER BY 1, 2",
        settings=settings,
    )
