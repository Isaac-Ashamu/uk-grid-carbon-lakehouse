"""Publish the dbt marts from DuckDB into Postgres for serving.

Two load modes, chosen per table in ``MART_TABLES``:

* ``upsert``  COPY into a temp table, then ``INSERT ... ON CONFLICT (keys) DO UPDATE``.
              Used for keyed facts and daily aggregates so a backfill of one day
              never touches another day's rows.
* ``swap``    COPY into ``<table>__next`` then rename it over the live table in
              one transaction. Used for small, fully recomputed tables (dimensions
              and history-wide aggregates) so readers never see a partial table.

Column types are derived from DuckDB's catalogue, so a new column in a dbt
model shows up in Postgres on the next load without hand-written DDL.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg import sql

from gridcarbon import warehouse
from gridcarbon.config import Settings, get_settings
from gridcarbon.logging import get_logger

log = get_logger(__name__)

BATCH_ROWS = 20_000


@dataclass(frozen=True)
class MartSpec:
    name: str
    mode: str  # "upsert" | "swap"
    keys: tuple[str, ...] = ()


MART_TABLES: tuple[MartSpec, ...] = (
    MartSpec("dim_region", "swap"),
    MartSpec("dim_fuel", "swap"),
    MartSpec("dim_fuel_factors", "swap"),
    MartSpec("dim_date", "upsert", ("date_day",)),
    MartSpec("fct_intensity_national_halfhourly", "upsert", ("period_start",)),
    MartSpec("fct_intensity_regional_halfhourly", "upsert", ("period_start", "region_id")),
    MartSpec("agg_daily_national", "upsert", ("date_day",)),
    MartSpec("agg_daily_regional", "upsert", ("date_day", "region_id")),
    MartSpec("agg_forecast_accuracy_by_settlement_period", "swap"),
    MartSpec("agg_hour_of_day_regional", "swap"),
    MartSpec("agg_wind_speed_bins_regional", "swap"),
    MartSpec("agg_weather_intensity_correlation", "swap"),
    MartSpec("data_quality_daily", "upsert", ("date_day", "dataset")),
)

_TYPE_MAP: dict[str, str] = {
    "BOOLEAN": "boolean",
    "TINYINT": "smallint",
    "SMALLINT": "smallint",
    "INTEGER": "integer",
    "BIGINT": "bigint",
    "HUGEINT": "numeric",
    "UTINYINT": "smallint",
    "USMALLINT": "integer",
    "UINTEGER": "bigint",
    "UBIGINT": "numeric",
    "FLOAT": "real",
    "DOUBLE": "double precision",
    "VARCHAR": "text",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamptz",
    "TIMESTAMP_NS": "timestamp",
    "TIME": "time",
    "JSON": "jsonb",
}


def _pg_type(duck_type: str) -> str:
    upper = duck_type.upper()
    if upper.startswith("DECIMAL"):
        return "numeric"
    return _TYPE_MAP.get(upper, "text")


def _clean(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _columns(duck: Any, table: str) -> list[tuple[str, str]]:
    rows = duck.execute(f"DESCRIBE marts.{table}").fetchall()
    return [(name, _pg_type(dtype)) for name, dtype, *_ in rows]


def _create_table(
    cur: psycopg.Cursor[Any],
    schema: str,
    name: str,
    columns: list[tuple[str, str]],
    keys: tuple[str, ...],
) -> None:
    cols: list[sql.Composable] = [
        sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t)) for c, t in columns
    ]
    cols.append(sql.SQL("loaded_at timestamptz not null default now()"))
    if keys:
        cols.append(
            sql.SQL("primary key ({})").format(sql.SQL(", ").join(sql.Identifier(k) for k in keys))
        )
    cur.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema), sql.Identifier(name), sql.SQL(", ").join(cols)
        )
    )


def _ensure_columns(
    cur: psycopg.Cursor[Any], schema: str, name: str, columns: list[tuple[str, str]]
) -> None:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, name),
    )
    existing = {r[0] for r in cur.fetchall()}
    for col, typ in columns:
        if col not in existing:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                    sql.Identifier(schema), sql.Identifier(name), sql.Identifier(col), sql.SQL(typ)
                )
            )


def _copy_rows(
    cur: psycopg.Cursor[Any],
    schema: str,
    name: str,
    columns: list[tuple[str, str]],
    duck: Any,
    table: str,
) -> int:
    col_idents = sql.SQL(", ").join(sql.Identifier(c) for c, _ in columns)
    select_cols = ", ".join(f'"{c}"' for c, _ in columns)
    duck.execute(f"SELECT {select_cols} FROM marts.{table}")
    total = 0
    with cur.copy(
        sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
            sql.Identifier(schema), sql.Identifier(name), col_idents
        )
    ) as copy:
        while True:
            batch = duck.fetchmany(BATCH_ROWS)
            if not batch:
                break
            for row in batch:
                copy.write_row(tuple(_clean(v) for v in row))
            total += len(batch)
    return total


def _load_upsert(
    conn: psycopg.Connection[Any],
    schema: str,
    spec: MartSpec,
    columns: list[tuple[str, str]],
    duck: Any,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (schema, spec.name),
        )
        if cur.fetchone() is None:
            _create_table(cur, schema, spec.name, columns, spec.keys)
        else:
            _ensure_columns(cur, schema, spec.name, columns)
        tmp = f"{spec.name}__incoming"
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(tmp)))
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {}.{} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                sql.Identifier(tmp), sql.Identifier(schema), sql.Identifier(spec.name)
            )
        )
        # Temp tables are unqualified; reuse the copy helper by targeting pg_temp.
        rows = _copy_rows(cur, "pg_temp", tmp, columns, duck, spec.name)
        updates = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c, _ in columns
            if c not in spec.keys
        )
        cur.execute(
            sql.SQL(
                "INSERT INTO {schema}.{name} ({cols}, loaded_at) "
                "SELECT {cols}, now() FROM pg_temp.{tmp} "
                "ON CONFLICT ({keys}) DO UPDATE SET {updates}, loaded_at = now()"
            ).format(
                schema=sql.Identifier(schema),
                name=sql.Identifier(spec.name),
                cols=sql.SQL(", ").join(sql.Identifier(c) for c, _ in columns),
                tmp=sql.Identifier(tmp),
                keys=sql.SQL(", ").join(sql.Identifier(k) for k in spec.keys),
                updates=updates,
            )
        )
    conn.commit()
    return rows


def _load_swap(
    conn: psycopg.Connection[Any],
    schema: str,
    spec: MartSpec,
    columns: list[tuple[str, str]],
    duck: Any,
) -> int:
    nxt = f"{spec.name}__next"
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(schema), sql.Identifier(nxt)
            )
        )
        _create_table(cur, schema, nxt, columns, ())
        rows = _copy_rows(cur, schema, nxt, columns, duck, spec.name)
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(schema), sql.Identifier(spec.name)
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                sql.Identifier(schema), sql.Identifier(nxt), sql.Identifier(spec.name)
            )
        )
    conn.commit()
    return rows


def load_marts(settings: Settings | None = None, tables: list[str] | None = None) -> dict[str, int]:
    settings = settings or get_settings()
    if not settings.postgres_dsn:
        raise RuntimeError("GRIDCARBON_POSTGRES_DSN is not set")
    specs = [s for s in MART_TABLES if tables is None or s.name in tables]
    counts: dict[str, int] = {}
    started = datetime.now(tz=UTC)
    with (
        warehouse.connect(settings, read_only=True) as duck,
        psycopg.connect(settings.postgres_dsn) as conn,
    ):
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(settings.postgres_schema)
                )
            )
        conn.commit()
        for spec in specs:
            columns = _columns(duck, spec.name)
            loader = _load_upsert if spec.mode == "upsert" else _load_swap
            rows = loader(conn, settings.postgres_schema, spec, columns, duck)
            counts[spec.name] = rows
            log.info("postgres.loaded", table=spec.name, mode=spec.mode, rows=rows)
    log.info(
        "postgres.finished",
        tables=len(counts),
        rows=sum(counts.values()),
        elapsed_s=round((datetime.now(tz=UTC) - started).total_seconds(), 2),
    )
    return counts


def table_counts(settings: Settings | None = None) -> dict[str, int]:
    settings = settings or get_settings()
    if not settings.postgres_dsn:
        raise RuntimeError("GRIDCARBON_POSTGRES_DSN is not set")
    out: dict[str, int] = {}
    with psycopg.connect(settings.postgres_dsn) as conn, conn.cursor() as cur:
        for spec in MART_TABLES:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (settings.postgres_schema, spec.name),
            )
            if cur.fetchone() is None:
                continue
            cur.execute(
                sql.SQL("SELECT count(*) FROM {}.{}").format(
                    sql.Identifier(settings.postgres_schema), sql.Identifier(spec.name)
                )
            )
            row = cur.fetchone()
            out[spec.name] = int(row[0]) if row else 0
    return out
