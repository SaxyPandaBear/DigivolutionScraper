import logging
from unittest.mock import MagicMock, patch

from pymongo import ReplaceOne

from tasks.load_to_mongo import MONGO_COLLECTION, MONGO_DB, load_to_mongo

# mongomock's bulk_write/ReplaceOne support hasn't caught up to this project's
# pinned pymongo version (see tests/unit/test_reconcile_mongo.py for context),
# so this asserts on the boundary call instead of simulating a real database.


def _mock_client():
    mock_collection = MagicMock()
    mock_collection.bulk_write.return_value = MagicMock(upserted_count=0, modified_count=0)
    mock_client = MagicMock()
    mock_client.__getitem__.return_value.__getitem__.return_value = mock_collection
    return mock_client, mock_collection


def test_load_to_mongo_upserts_each_flattened_document_by_id():
    mock_client, mock_collection = _mock_client()
    batch1 = [{"_id": "agumon", "name": "Agumon"}]
    batch2 = [{"_id": "gabumon", "name": "Gabumon"}]

    with patch("tasks.load_to_mongo.MongoClient", return_value=mock_client):
        count = load_to_mongo.function([batch1, batch2], logging.getLogger("test"))

    assert count == 2
    mock_client.__getitem__.assert_any_call(MONGO_DB)
    mock_collection.bulk_write.assert_called_once()
    args, kwargs = mock_collection.bulk_write.call_args
    assert args[0] == [
        ReplaceOne({"_id": "agumon"}, {"_id": "agumon", "name": "Agumon"}, upsert=True),
        ReplaceOne({"_id": "gabumon"}, {"_id": "gabumon", "name": "Gabumon"}, upsert=True),
    ]
    assert kwargs == {"ordered": False}
    mock_client.close.assert_called_once()


def test_load_to_mongo_skips_bulk_write_when_nothing_scraped():
    mock_client, mock_collection = _mock_client()

    with patch("tasks.load_to_mongo.MongoClient", return_value=mock_client):
        count = load_to_mongo.function([[], []], logging.getLogger("test"))

    assert count == 0
    mock_collection.bulk_write.assert_not_called()
    mock_client.close.assert_called_once()


def test_load_to_mongo_closes_client_even_if_bulk_write_raises():
    mock_client, mock_collection = _mock_client()
    mock_collection.bulk_write.side_effect = RuntimeError("boom")

    with patch("tasks.load_to_mongo.MongoClient", return_value=mock_client):
        try:
            load_to_mongo.function([[{"_id": "agumon", "name": "Agumon"}]], logging.getLogger("test"))
        except RuntimeError:
            pass

    mock_client.close.assert_called_once()
