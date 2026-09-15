"""Run audit trail.

Every stage of every run appends one JSON line to ``audit/runs.jsonl`` with
its status, timing, row counts and error (if any). The warehouse exposes the
file as ``audit.pipeline_runs`` so freshness, failure rate and quarantine
volume are queryable in SQL next to the data they describe.
"""

from __future__ import annotations

import json
import traceback
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from gridcarbon.config import Settings, get_settings
from gridcarbon.logging import get_logger

log = get_logger(__name__)


def new_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]


@dataclass
class StageRun:
    run_id: str
    stage: str
    dataset: str | None
    partition: str | None
    started_at: str
    finished_at: str | None = None
    status: str = "running"
    duration_ms: int | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    rows_quarantined: int | None = None
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def set(self, **metrics: Any) -> None:
        for key, value in metrics.items():
            if key in {"rows_in", "rows_out", "rows_quarantined"}:
                setattr(self, key, value)
            else:
                self.details[key] = value


class AuditLog:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.path: Path = self.settings.audit_dir / "runs.jsonl"

    def append(self, run: StageRun) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(asdict(run), sort_keys=True) + "\n")

    def read(self) -> list[StageRun]:
        if not self.path.exists():
            return []
        out: list[StageRun] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                out.append(StageRun(**json.loads(line)))
        return out

    @contextmanager
    def stage(
        self, run_id: str, stage: str, dataset: str | None = None, partition: str | None = None
    ) -> Iterator[StageRun]:
        run = StageRun(
            run_id=run_id,
            stage=stage,
            dataset=dataset,
            partition=partition,
            started_at=datetime.now(tz=UTC).isoformat(),
        )
        started = datetime.now(tz=UTC)
        log.info("stage.started", stage=stage, dataset=dataset, partition=partition)
        try:
            yield run
        except Exception as exc:
            run.status = "failed"
            run.error = f"{type(exc).__name__}: {exc}"
            run.details["traceback"] = traceback.format_exc()[-2000:]
            log.error(
                "stage.failed", stage=stage, dataset=dataset, partition=partition, error=run.error
            )
            raise
        else:
            if run.status == "running":
                run.status = "succeeded"
            log.info(
                "stage.finished",
                stage=stage,
                dataset=dataset,
                partition=partition,
                status=run.status,
                rows_in=run.rows_in,
                rows_out=run.rows_out,
                rows_quarantined=run.rows_quarantined,
            )
        finally:
            finished = datetime.now(tz=UTC)
            run.finished_at = finished.isoformat()
            run.duration_ms = int((finished - started).total_seconds() * 1000)
            self.append(run)
