"""
Runs the whole DAG in-process with `dag.test()` - every task, the dynamic
mapping, XCom passing between tasks and the short-circuit - against a local
fake of digimon.net (tests/fake_site) and a real, throwaway MongoDB container.
Needs Docker, but no network access to the real site.

A real Mongo rather than mongomock: mongomock can't execute load_to_mongo's
bulk_write/ReplaceOne against this project's pymongo version.

This doesn't exercise the deployment's process model (scripts/run_dag_once.sh,
api-server + scheduler as separate processes) - see test_end_to_end.py for that.
"""

import threading
from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.utils.db import initdb
from airflow.utils.state import DagRunState, TaskInstanceState
from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

import tasks.check_registration_count
import tasks.load_to_mongo
import tasks.reconcile_mongo
import tasks.scrape_digimon
from names import digimon_names
from tests.fake_site.server import make_server

DAG_FILE = str(Path(__file__).parent.parent.parent / "digivolution_dag.py")
DAG_ID = "digivolution_scraper"
DOWNSTREAM_OF_CHECK = {"validate_references", "scrape_digimon", "load_to_mongo", "reconcile_mongo"}


@pytest.fixture(scope="module")
def dag():
    initdb()
    return DagBag(dag_folder=DAG_FILE).dags[DAG_ID]


@pytest.fixture(scope="module")
def mongo_uri():
    with MongoDbContainer("mongo:7") as container:
        yield container.get_connection_url()


@pytest.fixture
def collection(mongo_uri, monkeypatch):
    for module in (tasks.check_registration_count, tasks.load_to_mongo, tasks.reconcile_mongo):
        monkeypatch.setattr(module, "MONGO_URI", mongo_uri)
    client = MongoClient(mongo_uri)
    coll = client[tasks.load_to_mongo.MONGO_DB][tasks.load_to_mongo.MONGO_COLLECTION]
    coll.delete_many({})
    yield coll
    client.close()


@pytest.fixture
def fake_site(monkeypatch):
    servers = []

    def start(registered_count=None):
        server = make_server(digimon_names, port=0, registered_count=registered_count)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        base = f"http://127.0.0.1:{server.server_address[1]}"
        monkeypatch.setattr(tasks.scrape_digimon, "url_template", f"{base}/reference_en/detail.php?directory_name=")
        monkeypatch.setattr(tasks.scrape_digimon, "img_domain", f"{base}/")
        monkeypatch.setattr(tasks.check_registration_count, "REFERENCE_URL", f"{base}/reference_en/")
        return base

    yield start
    for server in servers:
        server.shutdown()
        server.server_close()


def _task_states(dag_run) -> dict[str, set[str]]:
    states: dict[str, set[str]] = {}
    for ti in dag_run.get_task_instances():
        states.setdefault(ti.task_id, set()).add(ti.state)
    return states


def test_full_run_scrapes_every_digimon_into_mongo(dag, collection, fake_site):
    base = fake_site()

    dag_run = dag.test()

    assert dag_run.state == DagRunState.SUCCESS
    assert all(states == {TaskInstanceState.SUCCESS} for states in _task_states(dag_run).values())
    assert collection.count_documents({}) == len(digimon_names)
    agumon = collection.find_one({"_id": "agumon"})
    assert agumon["name"] == "Agumon"
    assert agumon["moves"] == ["Move A", "Move B"]
    assert agumon["img_src"] == f"{base}/cimages/digimon/agumon.jpg"
    assert "next_digivolutions" in agumon


def test_skips_everything_downstream_when_mongo_is_already_up_to_date(dag, collection, fake_site):
    fake_site()
    collection.insert_many([{"_id": name} for name in digimon_names])

    dag_run = dag.test()

    states = _task_states(dag_run)
    assert dag_run.state == DagRunState.SUCCESS
    assert states["check_registration_count"] == {TaskInstanceState.SUCCESS}
    for task_id in DOWNSTREAM_OF_CHECK:
        assert states[task_id] == {TaskInstanceState.SKIPPED}, task_id
    # untouched: still the bare seeded documents, not scraped ones
    assert collection.find_one({"_id": "agumon"}) == {"_id": "agumon"}


def test_fails_fast_when_names_py_is_out_of_sync_with_the_site(dag, collection, fake_site):
    fake_site(registered_count=len(digimon_names) + 1)

    dag_run = dag.test()

    states = _task_states(dag_run)
    assert dag_run.state == DagRunState.FAILED
    assert states["check_registration_count"] == {TaskInstanceState.FAILED}
    for task_id in DOWNSTREAM_OF_CHECK:
        assert states[task_id] == {TaskInstanceState.UPSTREAM_FAILED}, task_id
    assert collection.count_documents({}) == 0
