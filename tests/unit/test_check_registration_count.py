import logging
from pathlib import Path
from unittest.mock import patch

import mongomock
import pytest

from tasks.check_registration_count import (
    MONGO_COLLECTION,
    MONGO_DB,
    MONGO_URI,
    REFERENCE_URL,
    check_registration_count,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
REFERENCE_INDEX_HTML = (FIXTURES_DIR / "reference_index.html").read_text()  # encodes count "1317"


@pytest.fixture
def mongo_client():
    client = mongomock.MongoClient(MONGO_URI)
    with patch("tasks.check_registration_count.MongoClient", return_value=client):
        yield client
    client.close()


def _collection(client):
    return client[MONGO_DB][MONGO_COLLECTION]


def test_raises_when_names_py_is_out_of_sync_with_the_site(requests_mock, mongo_client):
    requests_mock.get(REFERENCE_URL, text=REFERENCE_INDEX_HTML)
    names = ["digimon"] * 1000  # deliberately not 1317

    with pytest.raises(ValueError, match="names.py only has 1000"):
        check_registration_count.function(names, logging.getLogger("test"))


def test_raises_on_unparseable_count(requests_mock):
    requests_mock.get(REFERENCE_URL, text="<html><body>no counter here</body></html>")

    with pytest.raises(ValueError, match="Couldn't parse"):
        check_registration_count.function(["digimon"], logging.getLogger("test"))


def test_returns_false_when_mongo_already_matches(requests_mock, mongo_client):
    requests_mock.get(REFERENCE_URL, text=REFERENCE_INDEX_HTML)
    names = ["digimon"] * 1317
    _collection(mongo_client).insert_many([{"_id": str(i)} for i in range(1317)])

    result = check_registration_count.function(names, logging.getLogger("test"))

    assert result is False


def test_returns_true_when_mongo_is_behind(requests_mock, mongo_client):
    requests_mock.get(REFERENCE_URL, text=REFERENCE_INDEX_HTML)
    names = ["digimon"] * 1317
    _collection(mongo_client).insert_many([{"_id": str(i)} for i in range(1300)])

    result = check_registration_count.function(names, logging.getLogger("test"))

    assert result is True
