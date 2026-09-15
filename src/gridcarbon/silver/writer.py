"""Silver layer storage: Hive-partitioned Parquet with partition-overwrite semantics.

    silver/<table>/date=<YYYY-MM-DD>/part-0.parquet
    silver/quarantine/date=<YYYY-MM-DD>/<table>.parquet

Re-processing a partition replaces exactly that partition's files and nothing
else, which is what makes ``transform`` idempotent and backfills safe to
re-run. Writes go to a temporary file and are renamed into place so a reader
never sees a half-written Parquet file.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from gridcarbon.config import Settings, get_settings
from gridcarbon.domain import Partition
from gridcarbon.logging import get_logger
from gridcarbon.silver.parsers import ParsedTable
from gridcarbon.silver.schemas import SCHEMAS

log = get_logger(__name__)


def table_dir(settings: Settings, table: str) -> Path:
    return settings.silver_dir / table


def partition_path(settings: Settings, table: str, partition: Partition) -> Path:
    return table_dir(settings, table) / f"date={partition.key}"


def _write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)


def write_silver_partition(
    parsed: ParsedTable, partition: Partition, settings: Settings | None = None
) -> Path:
    """Overwrite ``parsed.name``'s partition with ``parsed.frame``; return the written file."""
    settings = settings or get_settings()
    if parsed.name not in SCHEMAS:
        raise KeyError(f"unknown silver table {parsed.name!r}")
    target_dir = partition_path(settings, parsed.name, partition)
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    out = target_dir / "part-0.parquet"
    _write_parquet(parsed.frame, out)
    log.info("silver.written", table=parsed.name, partition=partition.key, rows=len(parsed.frame))
    return out


def write_quarantine(
    parsed: ParsedTable, partition: Partition, settings: Settings | None = None
) -> Path | None:
    settings = settings or get_settings()
    target_dir = partition_path(settings, "quarantine", partition)
    out = target_dir / f"{parsed.name}.parquet"
    if parsed.quarantine.empty:
        if out.exists():
            out.unlink()  # a clean re-run clears the previous run's rejects
        return None
    _write_parquet(parsed.quarantine, out)
    log.warning(
        "silver.quarantined",
        table=parsed.name,
        partition=partition.key,
        rows=len(parsed.quarantine),
        path=str(out),
    )
    return out


def read_silver(
    table: str, partitions: list[Partition] | None = None, settings: Settings | None = None
) -> pd.DataFrame:
    """Read a silver table (optionally only some partitions) back into pandas."""
    settings = settings or get_settings()
    root = table_dir(settings, table)
    if not root.exists():
        return pd.DataFrame(columns=list(SCHEMAS[table].columns))
    files: list[Path]
    if partitions is None:
        files = sorted(root.glob("date=*/*.parquet"))
    else:
        files = [
            f
            for p in partitions
            for f in sorted(partition_path(settings, table, p).glob("*.parquet"))
        ]
    if not files:
        return pd.DataFrame(columns=list(SCHEMAS[table].columns))
    frames = [pq.read_table(f).to_pandas() for f in files]
    return pd.DataFrame(pd.concat(frames, ignore_index=True))


def list_silver_partitions(table: str, settings: Settings | None = None) -> list[Partition]:
    settings = settings or get_settings()
    root = table_dir(settings, table)
    if not root.exists():
        return []
    return sorted(
        Partition.parse(p.name.removeprefix("date="))
        for p in root.iterdir()
        if p.is_dir() and p.name.startswith("date=") and any(p.glob("*.parquet"))
    )
