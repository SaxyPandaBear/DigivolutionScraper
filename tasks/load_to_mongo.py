import logging
import os

from airflow.sdk import task
from pymongo import MongoClient, ReplaceOne

# Connection is fully configurable via environment variables so the same DAG
# code can run locally (e.g. against the docker-compose MongoDB) or in
# production against the real cluster.
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "digimon")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "digimon")


@task
def load_to_mongo(batches: list[list[dict]], logger: logging.Logger) -> int:
    documents = [digimon for batch in batches for digimon in batch]

    logger.info(f"Connecting to MongoDB at {MONGO_URI}...")
    client = MongoClient(MONGO_URI)
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]

        if documents:
            # each document's `_id` is the Digimon's directory name, which is
            # what the collection relies on for uniqueness - replace the
            # existing document for that id, or insert it if it's new.
            operations = [ReplaceOne({"_id": digimon["_id"]}, digimon, upsert=True) for digimon in documents]
            result = collection.bulk_write(operations, ordered=False)
            logger.info(
                f"Upserted {len(documents)} Digimon into {MONGO_DB}.{MONGO_COLLECTION} "
                f"({result.upserted_count} inserted, {result.modified_count} updated)"
            )
        else:
            logger.info(f"No Digimon scraped; nothing to upsert into {MONGO_DB}.{MONGO_COLLECTION}")
    finally:
        client.close()

    return len(documents)
