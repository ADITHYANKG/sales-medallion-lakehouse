"""DAG structure tests (run where Airflow is installed, i.e. inside the Docker image)."""
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAG_FOLDER = Path(__file__).resolve().parents[2] / "airflow" / "dags"
EXPECTED_ORDER = ["check_source", "generate_or_fetch_sales", "upload_bronze", "validate_bronze",
                  "transform_silver", "validate_silver", "transform_gold", "load_data_warehouse",
                  "verify_warehouse", "record_pipeline_metrics"]


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)
    assert bag.import_errors == {}
    return bag.dags["sales_data_pipeline"]  # parsed object; no metadata DB needed


def test_tasks_and_linear_dependencies(dag):
    assert set(dag.task_ids) == set(EXPECTED_ORDER)
    for upstream, downstream in zip(EXPECTED_ORDER, EXPECTED_ORDER[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids, (upstream, downstream)


def test_reliability_settings(dag):
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    for task in dag.tasks:
        assert task.execution_timeout is not None
        assert task.on_failure_callback
    assert dag.get_task("transform_silver").retries == 2
    assert dag.get_task("validate_silver").retries == 0  # quality gates fail fast
    for name in ("upload_bronze", "transform_silver", "validate_silver", "transform_gold", "load_data_warehouse"):
        assert dag.get_task(name).pool == "pipeline_writes", name


def test_params(dag):
    assert dag.params["source_mode"] == "auto"
