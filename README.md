Digivolution Scraper
====================

Automation workflow that joins known/predetermined evolution data from multiple sources with
official metadata, and then pushes that to MongoDB. This acts as a data pipeline that services
[DigimonQL](https://github.com/SaxyPandaBear/DigimonQL).

# How it works

The pipeline is a single Airflow DAG, `digivolution_scraper` (defined in `digivolution_dag.py`),
made up of three tasks:

1. **`validate_references`** (`tasks/validate_references.py`) — sanity-checks the hand-maintained
   data before anything is scraped: no duplicate names in `names.py`, every name referenced in
   `evolutions.py` / `modes.py` is a real, known Digimon, and (as a soft check) reports how many
   Digimon still have no evolution mapping at all. Fails the DAG run early if the bootstrapping
   data is bad, rather than burning time scraping first.

2. **`scrape_digimon`** (`tasks/scrape_digimon.py`) — fetches and cleans the official detail page
   for each Digimon from `digimon.net`. Rather than one task per Digimon (which would mean one
   mapped task instance per Digimon), the ~1300 names in `names.py` are split into batches of 25
   and the task is dynamically mapped (`.expand()`) over those batches, so batches scrape in
   parallel while requests within a batch stay sequential and rate-limited. Each mapped instance
   returns a list of cleaned Digimon dicts (level, type, attribute, moves, image URL, background
   text, mode/X-antibody flags, and forward/backward evolution links derived from `evolutions.py`
   and `modes.py`). The task retries up to 3 times with exponential backoff on failure.

3. **`load_to_mongo`** (`tasks/load_to_mongo.py`) — collects every batch's output, flattens it into
   one list of documents, and performs a full drop-and-load of the configured MongoDB collection
   (drop the collection, then insert everything scraped in this run). This keeps the collection
   from ever containing a mix of old and new data.

Task order: `validate_references` → `scrape_digimon` (mapped) → `load_to_mongo`.

The evolution/mode data in `evolutions.py` and `modes.py` is hand-maintained and unidirectional
(only "evolves into" links are written by hand); the DAG derives the inverse "evolves from" /
mode-variant-of relationships automatically at parse time via `derive_inverse_relationship`.

# Configuration

`load_to_mongo` reads its MongoDB connection from environment variables, so the same DAG code
runs unmodified locally or in production:

| Variable            | Default                     | Purpose                              |
|----------------------|------------------------------|---------------------------------------|
| `MONGO_URI`          | `mongodb://localhost:27017` | Full MongoDB connection string       |
| `MONGO_DB`           | `digimon`                   | Target database name                 |
| `MONGO_COLLECTION`   | `digimon`                   | Target collection name (drop-and-load) |

# Running

> Note: This was developed with Python 3.14.6 and Airflow 3.3.0. Deviating from this may create
> unexpected behavior.

Run the whole project (Airflow + MongoDB) via Docker:
```bash
docker compose up
```
This starts:
- `mongo` — MongoDB on `localhost:27017`, matching the config defaults above so no extra setup
  is needed.
- `airflow` — a single-container `airflow standalone` instance (webserver, scheduler, and
  triggerer) on `localhost:8080`, with `MONGO_URI` pointed at the `mongo` service. The DAG source
  files are bind-mounted in, so local edits are picked up without rebuilding the image.

On first boot, Airflow generates a random admin password. Grab it from the logs:
```bash
docker compose logs airflow | grep password
```
Then log in at `http://localhost:8080` and trigger `digivolution_scraper` manually (it also runs
on its own schedule — every 4 weeks). Note that a full run scrapes every Digimon in `names.py`
(~1300 pages), so expect it to take a while.

Local development in a virtualenv (for editing/testing task code without running Airflow itself):
```bash
source bin/activate
pip install -r requirements.txt
```

# Testing
TBD
