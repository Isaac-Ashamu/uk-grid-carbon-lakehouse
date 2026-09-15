import pytest

from gridcarbon.audit import AuditLog, new_run_id
from gridcarbon.config import Settings


def test_stage_records_success_and_failure(settings: Settings) -> None:
    log = AuditLog(settings)
    run_id = new_run_id()
    with log.stage(run_id, "extract", "national_intensity", "2026-09-01") as s:
        s.set(rows_out=48, bytes=1000)
    with (
        pytest.raises(ValueError, match="bad"),
        log.stage(run_id, "transform", "national_intensity", "2026-09-01"),
    ):
        raise ValueError("bad day")

    runs = log.read()
    assert [r.status for r in runs] == ["succeeded", "failed"]
    assert runs[0].rows_out == 48 and runs[0].details["bytes"] == 1000
    assert runs[1].error == "ValueError: bad day"
    assert "Traceback" in runs[1].details["traceback"]
    assert all(r.duration_ms is not None and r.finished_at for r in runs)
