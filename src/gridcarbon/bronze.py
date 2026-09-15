"""Bronze layer: the raw API responses, byte for byte, with provenance.

Layout (Hive-style so DuckDB and Spark can both read it):

    bronze/source=<source>/dataset=<dataset>/date=<YYYY-MM-DD>/<fetched_at>-<sha8>.json
    bronze/source=<source>/dataset=<dataset>/date=<YYYY-MM-DD>/<fetched_at>-<sha8>.manifest.json

Rules:

* Append-only. A re-run for the same partition adds a new file; nothing is
  ever modified or deleted. Silver decides which fetch to trust (the latest).
* Byte-identical responses are not stored twice: if a file with the same
  SHA-256 already exists in the partition, the write is a no-op and the
  existing record is returned. That makes ``extract`` safe to retry.
* The manifest sidecar records the URL, request parameters, status, size,
  hash, timing, attempts and run id so any silver row can be traced back to
  the exact HTTP response it came from.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from gridcarbon.config import Settings, get_settings
from gridcarbon.domain import Partition
from gridcarbon.logging import get_logger
from gridcarbon.sources.base import RawFetch

log = get_logger(__name__)


@dataclass(frozen=True)
class BronzeRecord:
    source: str
    dataset: str
    partition: str
    path: str
    sha256: str
    bytes: int
    fetched_at: str
    url: str
    status_code: int
    attempts: int
    elapsed_ms: int
    run_id: str
    request: dict[str, Any]
    deduplicated: bool = False

    def read_json(self) -> Any:
        return json.loads(Path(self.path).read_bytes())

    @property
    def fetched_at_dt(self) -> datetime:
        return datetime.fromisoformat(self.fetched_at)


def partition_dir(settings: Settings, source: str, dataset: str, partition: Partition) -> Path:
    return settings.bronze_dir / f"source={source}" / f"dataset={dataset}" / f"date={partition.key}"


def write_bronze(fetch: RawFetch, run_id: str, settings: Settings | None = None) -> BronzeRecord:
    settings = settings or get_settings()
    directory = partition_dir(settings, fetch.source, fetch.dataset, fetch.partition)
    directory.mkdir(parents=True, exist_ok=True)

    sha = fetch.result.sha256
    existing = _find_by_sha(directory, sha)
    if existing is not None:
        log.info(
            "bronze.deduplicated",
            dataset=fetch.dataset,
            partition=fetch.partition.key,
            sha256=sha[:12],
            path=existing.path,
        )
        return BronzeRecord(**{**asdict(existing), "deduplicated": True})

    stamp = fetch.result.fetched_at.strftime("%Y%m%dT%H%M%S%fZ")
    stem = f"{stamp}-{sha[:8]}"
    data_path = directory / f"{stem}.json"
    manifest_path = directory / f"{stem}.manifest.json"

    record = BronzeRecord(
        source=fetch.source,
        dataset=fetch.dataset,
        partition=fetch.partition.key,
        path=str(data_path),
        sha256=sha,
        bytes=len(fetch.result.body),
        fetched_at=fetch.result.fetched_at.isoformat(),
        url=fetch.result.url,
        status_code=fetch.result.status_code,
        attempts=fetch.result.attempts,
        elapsed_ms=fetch.result.elapsed_ms,
        run_id=run_id,
        request=fetch.request,
    )

    # Write data first, then the manifest: a manifest is the commit marker. A
    # crash between the two leaves an orphan data file that listing ignores.
    tmp = data_path.with_suffix(".json.tmp")
    tmp.write_bytes(fetch.result.body)
    tmp.replace(data_path)
    manifest_path.write_text(json.dumps(asdict(record), indent=2, sort_keys=True))

    log.info(
        "bronze.written",
        dataset=fetch.dataset,
        partition=fetch.partition.key,
        bytes=record.bytes,
        sha256=sha[:12],
        path=str(data_path),
    )
    return record


def list_bronze(
    source: str, dataset: str, partition: Partition, settings: Settings | None = None
) -> list[BronzeRecord]:
    """All committed fetches for a partition, oldest first."""
    settings = settings or get_settings()
    directory = partition_dir(settings, source, dataset, partition)
    if not directory.exists():
        return []
    records = [
        BronzeRecord(**json.loads(p.read_text()))
        for p in sorted(directory.glob("*.manifest.json"))
        if Path(json.loads(p.read_text())["path"]).exists()
    ]
    return sorted(records, key=lambda r: r.fetched_at)


def latest_bronze(
    source: str, dataset: str, partition: Partition, settings: Settings | None = None
) -> BronzeRecord | None:
    records = list_bronze(source, dataset, partition, settings)
    return records[-1] if records else None


def list_partitions(source: str, dataset: str, settings: Settings | None = None) -> list[Partition]:
    settings = settings or get_settings()
    root = settings.bronze_dir / f"source={source}" / f"dataset={dataset}"
    if not root.exists():
        return []
    return sorted(
        Partition.parse(p.name.removeprefix("date="))
        for p in root.iterdir()
        if p.is_dir() and p.name.startswith("date=") and any(p.glob("*.manifest.json"))
    )


def _find_by_sha(directory: Path, sha256: str) -> BronzeRecord | None:
    for manifest in directory.glob(f"*-{sha256[:8]}.manifest.json"):
        record = BronzeRecord(**json.loads(manifest.read_text()))
        if record.sha256 == sha256 and Path(record.path).exists():
            return record
    return None
