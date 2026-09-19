from pathlib import Path

import pytest
from airflow.models import DagBag

DAG_FILE = str(Path(__file__).parent.parent / "digivolution_dag.py")
DAG_ID = "digivolution_scraper"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAG_FILE)


def test_dag_has_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_is_loaded(dagbag):
    assert DAG_ID in dagbag.dags


def test_dag_has_expected_tasks(dagbag):
    dag = dagbag.dags[DAG_ID]

    assert set(dag.task_ids) == {
        "validate_references",
        "scrape_digimon",
        "load_to_mongo",
        "reconcile_mongo",
    }


def test_dag_task_dependencies_run_in_expected_order(dagbag):
    dag = dagbag.dags[DAG_ID]

    validate = dag.get_task("validate_references")
    scrape = dag.get_task("scrape_digimon")
    load = dag.get_task("load_to_mongo")
    reconcile = dag.get_task("reconcile_mongo")

    assert scrape.task_id in validate.downstream_task_ids
    assert load.task_id in scrape.downstream_task_ids
    assert reconcile.task_id in scrape.downstream_task_ids
    assert reconcile.task_id in load.downstream_task_ids
