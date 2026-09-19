import logging
import os

from airflow.sdk import task
from pymongo import MongoClient

# Connection is fully configurable via environment variables so the same DAG
# code can run locally (e.g. against the docker-compose MongoDB) or in
# production against the real cluster.
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "public")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "digimon")


@task
def reconcile_mongo(batches: list[list[dict]], logger: logging.Logger) -> int:
    scraped_ids = [digimon["_id"] for batch in batches for digimon in batch]

    if not scraped_ids:
        logger.warning(
            f"No Digimon were scraped this run; skipping reconciliation of "
            f"{MONGO_DB}.{MONGO_COLLECTION} to avoid deleting everything."
        )
        return 0

    logger.info(f"Connecting to MongoDB at {MONGO_URI}...")
    client = MongoClient(MONGO_URI)
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]
        # remove documents whose _id wasn't produced by this run's scrape, e.g.
        # a Digimon that was removed from names.py since the last successful run
        result = collection.delete_many({"_id": {"$nin": scraped_ids}})
        logger.info(
            f"Reconciled {MONGO_DB}.{MONGO_COLLECTION}: removed {result.deleted_count} "
            f"stale Digimon not present in this run's scrape"
        )
    finally:
        client.close()

    return result.deleted_count
