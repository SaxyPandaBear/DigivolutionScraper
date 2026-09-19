import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

import requests
from airflow.sdk import task
from bs4 import BeautifulSoup

from modes import known_mode_variants

url_template = "https://digimon.net/reference_en/detail.php?directory_name="  # url param is the CASE SENSITIVE name of the digimon
img_domain = "https://digimon.net/"

# Tags to look up
parent_tag = "p-ref"  # encompassing class tag
en_name_tag = "c-titleSet__main"  # localized English name
info_tag = "p-ref__info"  # section that has details like level, type, attribute, and special move(s)
profile_tag = "p-ref__txt"  # description of the Digimon

# the site is I/O-bound to fetch from, not CPU-bound to parse, so fan requests
# within a batch out across threads instead of fetching sequentially. Kept
# modest (rather than higher) since each mapped task instance runs concurrently
# with others (AIRFLOW__CORE__PARALLELISM), and peak memory is the tighter
# constraint on a resource-limited host, not wall-clock time.
MAX_CONCURRENT_REQUESTS = 5
REQUEST_TIMEOUT = (5, 15)  # (connect, read) seconds - guards against a hung request tying up a worker slot


# input is in the form <img src="../cimages/digimon/bearcatmon.jpg" alt="">
# so take that and replace the beginning with the domain.
def derive_img_src(src: str) -> str:
    return src.replace("../", img_domain)


# Example: "・Penetrate Blow・Murderize Rush・Beardown Spinning Kick"
# Calling split() will keep the empty string at the beginning, but for futureproofing,
# use a conditional list comprehension instead of just dropping the first element.
def parse_special_moves(s: str) -> list:
    return [move.strip() for move in s.strip().split("・") if len(move.strip()) > 0]


# The In-Training levels use the roman numerals I and II, in Unicode,
# but these aren't intuitively queryable compared to the numeric 1 and 2.
def clean_level(s: str) -> str:
    return s.replace("Ⅰ", "1").replace("Ⅱ", "2").replace("(Xros Wars)", "")


def clean_name(s: str) -> str:
    return s.replace("：", ":")


def clean_attribute(s: str) -> str:
    if len(s) == 0:
        return "None"
    return s


def _scrape_one(session: requests.Session, name: str, mappings: dict[str, dict[str, list[str]]], logger: logging.Logger) -> dict:
    digimon_url = f"{url_template}{name}"
    logger.info(f"Checking {digimon_url}...")
    r = session.get(digimon_url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, features="html.parser")

    digimon = soup.find(class_=parent_tag)
    if digimon is None:
        logger.error(f"Couldn't find data for {name} at {digimon_url}")
        raise ValueError(f"Couldn't find data for {name} at {digimon_url}")

    english_name = clean_name(digimon.find(class_=en_name_tag).text)
    img_url = derive_img_src(digimon.find("img")["src"])  # pyright:ignore

    info = digimon.find(class_=info_tag)
    if info is None:
        logger.error(f"Couldn't find {info_tag} for {name}.")
        raise ValueError(f"Couldn't find {info_tag} for {name}.")

    # There should be 4 elements: Level, Type, Attribute, Special Moves,
    # and the last element is a single string which may contain multiple values delimited by a dot character
    values = [t.text for t in info.find_all("dd")]
    digimon_level = clean_level(values[0])
    digimon_type = values[1]
    digimon_attr = clean_attribute(values[2])
    digimon_moves = parse_special_moves(values[3])

    result = {}
    # identifier is the name used in the URL for the digimon - uses underscore prefix for MongoDB semantics
    result["_id"] = name
    result["name"] = english_name
    result["level"] = digimon_level
    result["type"] = digimon_type
    result["attribute"] = clean_attribute(digimon_attr)
    result["moves"] = digimon_moves
    result["img_src"] = img_url
    result["background"] = digimon.find(class_=profile_tag).text.strip()
    result["is_mode"] = name in known_mode_variants or " Mode" in english_name
    result["is_x_antibody"] = "(X Antibody)" in english_name

    digimon_modes = mappings["digimon_modes"]
    previous_evolutions = mappings["previous_evolutions"]
    next_evolutions = mappings["next_evolutions"]

    if name in digimon_modes:
        result["modes"] = digimon_modes[name]
    # don't assume that the digimon exists in the map
    if name in previous_evolutions:
        result["previous_digivolutions"] = previous_evolutions[name]
    if name in next_evolutions:
        result["next_digivolutions"] = next_evolutions[name]

    logger.info(f"Successfully scraped {name}")
    return result


@task(
    retries=3,
    retry_exponential_backoff=True,
    retry_delay=timedelta(seconds=30),
)
def scrape_digimon(names: list[str], mappings: dict[str, dict[str, list[str]]], logger: logging.Logger) -> list[dict]:
    results = []
    failures = []

    # one Session per batch reuses its connection pool (keep-alive) across every
    # request in the batch instead of paying a fresh TCP+TLS handshake each time
    with requests.Session() as session, ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        future_to_name = {executor.submit(_scrape_one, session, name, mappings, logger): name for name in names}
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                results.append(future.result())
            except (ValueError, requests.exceptions.RequestException) as e:
                logger.error(f"Failed to scrape {name}: {e}")
                failures.append(name)

    if failures:
        raise ValueError(f"Failed to scrape {len(failures)} Digimon in batch: {failures}")

    return results
