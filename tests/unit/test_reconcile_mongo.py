import logging
from unittest.mock import patch

import mongomock
import pytest

from tasks.reconcile_mongo import MONGO_COLLECTION, MONGO_DB, MONGO_URI, reconcile_mongo


@pytest.fixture
def mongo_client():
    client = mongomock.MongoClient(MONGO_URI)
    with patch("tasks.reconcile_mongo.MongoClient", return_value=client):
        yield client
    client.close()


def _collection(client):
    return client[MONGO_DB][MONGO_COLLECTION]


def test_reconcile_mongo_deletes_documents_not_in_this_runs_scrape(mongo_client):
    _collection(mongo_client).insert_many(
        [
            {"_id": "agumon", "name": "Agumon"},
            {"_id": "removed_from_names_py", "name": "Stale"},
        ]
    )

    deleted = reconcile_mongo.function([[{"_id": "agumon", "name": "Agumon"}]], logging.getLogger("test"))

    assert deleted == 1
    remaining_ids = {d["_id"] for d in _collection(mongo_client).find({})}
    assert remaining_ids == {"agumon"}


def test_reconcile_mongo_is_a_noop_when_nothing_needs_removing(mongo_client):
    _collection(mongo_client).insert_one({"_id": "agumon", "name": "Agumon"})

    deleted = reconcile_mongo.function([[{"_id": "agumon", "name": "Agumon"}]], logging.getLogger("test"))

    assert deleted == 0
    assert _collection(mongo_client).count_documents({}) == 1


def test_reconcile_mongo_skips_deletion_when_scrape_produced_nothing(mongo_client):
    _collection(mongo_client).insert_one({"_id": "agumon", "name": "Agumon"})

    deleted = reconcile_mongo.function([[]], logging.getLogger("test"))

    assert deleted == 0
    # nothing should have been wiped out despite an empty scraped-id set
    assert _collection(mongo_client).count_documents({}) == 1
