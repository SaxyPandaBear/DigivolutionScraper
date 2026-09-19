import logging
import os

from airflow.sdk import task
from pymongo import MongoClient

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

        logger.info(f"Dropping existing {MONGO_DB}.{MONGO_COLLECTION} collection...")
        collection.drop()

        if documents:
            collection.insert_many(documents)

        logger.info(f"Successfully loaded {len(documents)} Digimon into {MONGO_DB}.{MONGO_COLLECTION}")
    finally:
        client.close()

    return len(documents)
