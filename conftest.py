import os
import shutil
import tempfile
from pathlib import Path

import pytest

# Airflow reads its config when first imported, so this has to run before any
# test module imports it. Gives every test session a throwaway Airflow home
# (sqlite metadata DB, DAGs folder holding only this project's DAG) instead of
# touching the developer's ~/airflow - and works on a fresh CI runner too.
_AIRFLOW_HOME = Path(tempfile.mkdtemp(prefix="airflow-test-"))
(_AIRFLOW_HOME / "dags").mkdir()
# a dedicated folder rather than the repo root, so DAG bundle syncing doesn't
# crawl the virtualenv's site-packages
(_AIRFLOW_HOME / "dags" / "digivolution_dag.py").symlink_to(Path(__file__).parent / "digivolution_dag.py")
os.environ["AIRFLOW_HOME"] = str(_AIRFLOW_HOME)
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(_AIRFLOW_HOME / "dags")
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{_AIRFLOW_HOME / 'airflow.db'}"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"


def pytest_unconfigure(config):
    shutil.rmtree(_AIRFLOW_HOME, ignore_errors=True)


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run the slow docker-compose integration test (builds and runs the real container stack)",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return
    skip_integration = pytest.mark.skip(reason="needs --run-integration (builds and runs the docker-compose stack)")
    for item in items:
        # get_closest_marker, not `"integration" in item.keywords` - keywords
        # also include directory names, which would skip all of tests/integration/
        if item.get_closest_marker("integration"):
            item.add_marker(skip_integration)
