"""DAG integrity: every DAG file imports, has no cycles, and looks the way the runbook says."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[2] / "airflow" / "dags"

# Configure Airflow before it is imported: a throwaway home, no example DAGs,
# and our dags folder on the path so sibling imports (gridcarbon_common) resolve
# the same way they do under the scheduler.
os.environ.setdefault(
    "AIRFLOW_HOME", str(Path(os.environ.get("TMPDIR", "/tmp")) / "gridcarbon-airflow-test")
)
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_DIR))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

airflow = pytest.importorskip(
    "airflow.models", reason="apache-airflow is installed in its own environment"
)


@pytest.fixture(scope="module")
def dagbag():  # type: ignore[no-untyped-def]
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False, read_dags_from_db=False)


def test_no_import_errors(dagbag) -> None:  # type: ignore[no-untyped-def]
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {
        "gridcarbon_daily",
        "gridcarbon_reference_weekly",
        "gridcarbon_api_smoke",
    }


def test_daily_dag_shape(dagbag) -> None:  # type: ignore[no-untyped-def]
    dag = dagbag.dags["gridcarbon_daily"]
    assert dag.catchup is True
    assert dag.max_active_runs == 3
    ids = {t.task_id for t in dag.tasks}
    assert {"build_marts", "quality_gate", "load_postgres"} <= ids
    assert sum(t.task_id.startswith("extract.") for t in dag.tasks) == 4
    assert sum(t.task_id.startswith("transform.") for t in dag.tasks) == 4
    assert dag.get_task("build_marts").pool == "gridcarbon_warehouse"
    assert dag.get_task("load_postgres").pool == "gridcarbon_warehouse"
    assert dag.get_task("load_postgres").upstream_task_ids == {"quality_gate"}
    assert all(t.retries >= 1 for t in dag.tasks)


def test_dataset_wiring(dagbag) -> None:  # type: ignore[no-untyped-def]
    daily = dagbag.dags["gridcarbon_daily"]
    smoke = dagbag.dags["gridcarbon_api_smoke"]
    outlets = [d.uri for d in daily.get_task("build_marts").outlets]
    assert outlets == ["gridcarbon://warehouse/marts"]
    assert smoke.timetable.summary == "Dataset"
