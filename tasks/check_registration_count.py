import logging
import os
from datetime import timedelta

import bs4
import requests
from airflow.sdk import task
from pymongo import MongoClient

# adapted from https://github.com/SaxyPandaBear/DigimonQL/blob/main/scraper/registrations.py
REFERENCE_URL = "https://digimon.net/reference_en/"
REQUEST_TIMEOUT = (5, 15)  # (connect, read) seconds

# same Mongo config surface as load_to_mongo.py/reconcile_mongo.py, so this
# checks the exact collection those tasks read from and write to
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "public")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "digimon")


def _scrape_registered_count(logger: logging.Logger) -> int:
    r = requests.get(REFERENCE_URL, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    soup = bs4.BeautifulSoup(r.text, features="html.parser")

    # the registered count is rendered as a sequence of digit images rather
    # than plain text; assumes they're in reading order, same as the site's
    # own markup relies on
    count_str = ""
    for elem in soup.find_all(class_="p-refCountNumList"):
        for child in elem.children:
            if isinstance(child, bs4.element.Tag):
                count_str += str(child.attrs["alt"])

    if not count_str.isdigit():
        raise ValueError(f"Couldn't parse a registered Digimon count from {REFERENCE_URL} (got {count_str!r})")

    registered_count = int(count_str)
    logger.info(f"The Digimon Encyclopedia indicates there are {registered_count} registered Digimon.")
    return registered_count


@task.short_circuit(
    retries=3,
    retry_exponential_backoff=True,
    retry_delay=timedelta(seconds=30),
)
def check_registration_count(names: list[str], logger: logging.Logger) -> bool:
    """
    Returns True to continue the DAG (there's new data to scrape), False to
    short-circuit and skip every downstream task (MongoDB is already
    up to date). Raises if the Encyclopedia's registered count disagrees
    with names.py, since that means the mapping data needs a manual update
    before scraping can proceed at all.
    """
    registered_count = _scrape_registered_count(logger)
    known_count = len(names)

    if registered_count != known_count:
        raise ValueError(
            f"The Digimon Encyclopedia reports {registered_count} registered Digimon, but "
            f"names.py only has {known_count} known names. The mapping data (names.py, "
            f"evolutions.py, modes.py) needs to be updated manually before this DAG can run."
        )

    logger.info(f"names.py is up to date with the Encyclopedia's {registered_count} registered Digimon.")

    client = MongoClient(MONGO_URI)
    try:
        mongo_count = client[MONGO_DB][MONGO_COLLECTION].count_documents({})
    finally:
        client.close()

    if mongo_count == registered_count:
        logger.info(f"MongoDB already has all {registered_count} Digimon - nothing to do.")
        return False

    logger.info(f"MongoDB has {mongo_count} of {registered_count} registered Digimon - continuing.")
    return True
