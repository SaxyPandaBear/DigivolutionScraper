"""
Opt-in, slow end-to-end test: brings up the full docker-compose stack (mongo +
postgres + airflow), triggers a real DAG run against the live digimon.net
site, waits for it to finish, and asserts the data landed in Mongo.

Not run by default - requires Docker and takes a few minutes. Run explicitly with:

    pytest --run-integration tests/integration/test_end_to_end.py
"""

import subprocess
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).parent.parent.parent
DAG_ID = "digivolution_scraper"
AIRFLOW_READY_TIMEOUT = 180
DAG_RUN_TIMEOUT = 600
EXPECTED_DIGIMON_COUNT = 1317


def _compose(*args, check=True):
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=check,
    )


def _airflow_cli(*args, check=True):
    return _compose("exec", "-T", "airflow", "airflow", *args, check=check)


def _wait_for_airflow_ready():
    deadline = time.time() + AIRFLOW_READY_TIMEOUT
    while time.time() < deadline:
        logs = _compose("logs", "airflow", check=False).stdout
        if "Airflow is ready" in logs:
            return
        time.sleep(5)
    raise TimeoutError(f"Airflow did not report ready within {AIRFLOW_READY_TIMEOUT}s")


def _latest_run_state() -> tuple[str, str]:
    result = _airflow_cli("dags", "list-runs", DAG_ID, "-o", "plain")
    lines = [line for line in result.stdout.splitlines() if line.startswith(DAG_ID)]
    if not lines:
        raise RuntimeError(f"No dag runs found for {DAG_ID} yet")
    fields = lines[-1].split()
    return fields[1], fields[2]  # run_id, state


def _wait_for_terminal_state() -> str:
    deadline = time.time() + DAG_RUN_TIMEOUT
    while time.time() < deadline:
        _, state = _latest_run_state()
        if state in ("success", "failed"):
            return state
        time.sleep(10)
    raise TimeoutError(f"DAG run did not reach a terminal state within {DAG_RUN_TIMEOUT}s")


@pytest.fixture(scope="module")
def running_stack():
    _compose("up", "-d", "--build")
    try:
        _wait_for_airflow_ready()
        yield
    finally:
        _compose("down", "-v")


def test_full_dag_run_scrapes_the_real_site_and_loads_mongo(running_stack):
    _airflow_cli("dags", "trigger", DAG_ID)

    final_state = _wait_for_terminal_state()
    assert final_state == "success"

    from pymongo import MongoClient

    client = MongoClient("mongodb://localhost:27017")
    try:
        count = client["digimon"]["digimon"].count_documents({})
        sample = client["digimon"]["digimon"].find_one({"_id": "agumon"})
    finally:
        client.close()

    assert count == EXPECTED_DIGIMON_COUNT
    assert sample is not None
    assert sample["name"] == "Agumon"
