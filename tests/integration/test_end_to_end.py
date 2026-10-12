"""
Opt-in, slow end-to-end test of the deployment's actual process model:
builds the image and runs scripts/run_dag_once.sh - the same entrypoint the
Railway cron job uses - in the docker-compose stack (postgres + mongo +
airflow), with digimon.net swapped for a local fake via
docker-compose.test.yml. No network access to the real site.

Complements test_pipeline.py, which runs the DAG in-process and so can't catch
problems in how the api-server/scheduler processes are started and wired up.

Not run by default - builds the image and takes a few minutes:

    pytest --run-integration tests/integration/test_end_to_end.py

Uses the base file's fixed container names and host ports, so stop a locally
running `docker compose up` stack first.
"""

import subprocess
from pathlib import Path

import pytest
from pymongo import MongoClient

from names import digimon_names

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).parent.parent.parent
COMPOSE = ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.test.yml"]
RUN_TIMEOUT_SECONDS = 900


def _compose(*args, check=True, timeout=None):
    return subprocess.run(
        [*COMPOSE, *args], cwd=REPO_ROOT, capture_output=True, text=True, check=check, timeout=timeout
    )


def _run_dag_once():
    return _compose(
        "run", "--rm", "airflow", "bash", "/opt/airflow/scripts/run_dag_once.sh",
        check=False, timeout=RUN_TIMEOUT_SECONDS,
    )


@pytest.fixture(scope="module")
def stack():
    _compose("build", "airflow")
    try:
        yield
    finally:
        _compose("down", "-v", check=False)


@pytest.fixture
def collection(stack):
    client = MongoClient("mongodb://localhost:27017")
    yield client["public"]["digimon"]
    client.close()


def test_cron_entrypoint_scrapes_into_mongo_then_short_circuits_on_rerun(collection):
    first = _run_dag_once()
    assert first.returncode == 0, first.stdout[-5000:] + first.stderr[-5000:]
    assert "Run finished: success" in first.stdout
    assert collection.count_documents({}) == len(digimon_names)
    assert collection.find_one({"_id": "agumon"})["name"] == "Agumon"

    # nothing changed on the (fake) site, so the second run should skip
    # straight past scraping - and still exit 0 for the cron job
    second = _run_dag_once()
    assert second.returncode == 0, second.stdout[-5000:] + second.stderr[-5000:]
    assert "nothing to do" in second.stdout
    assert "Run finished: success" in second.stdout
    assert collection.count_documents({}) == len(digimon_names)
