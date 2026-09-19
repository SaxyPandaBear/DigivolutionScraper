Digivolution Scraper
====================

Automation workflow that joins known/predetermined evolution data from multiple sources with
official metadata, and then pushes that to MongoDB. This acts as a data pipeline that services
[DigimonQL](https://github.com/SaxyPandaBear/DigimonQL).

# How it works

The pipeline is a single Airflow DAG, `digivolution_scraper` (defined in `digivolution_dag.py`),
made up of four tasks:

1. **`validate_references`** (`tasks/validate_references.py`) — sanity-checks the hand-maintained
   data before anything is scraped: no duplicate names in `names.py`, every name referenced in
   `evolutions.py` / `modes.py` is a real, known Digimon, and (as a soft check) reports how many
   Digimon still have no evolution mapping at all. Fails the DAG run early if the bootstrapping
   data is bad, rather than burning time scraping first.

2. **`scrape_digimon`** (`tasks/scrape_digimon.py`) — fetches and cleans the official detail page
   for each Digimon from `digimon.net`. Rather than one task per Digimon (which would mean one
   mapped task instance per Digimon), the ~1300 names in `names.py` are split into batches of 75
   and the task is dynamically mapped (`.expand()`) over those batches, so batches scrape in
   parallel across Airflow's own task concurrency. Within a batch, requests share one
   `requests.Session` (connection reuse) and fan out across a small thread pool (the site is
   I/O-bound to fetch from, not CPU-bound to parse, so concurrent requests are cheap), each with a
   connect/read timeout so one hung request can't tie up a worker slot indefinitely. Each mapped
   instance returns a list of cleaned Digimon dicts (level, type, attribute, moves, image URL,
   background text, mode/X-antibody flags, and forward/backward evolution links derived from
   `evolutions.py` and `modes.py`). The task retries up to 3 times with exponential backoff on
   failure.

3. **`load_to_mongo`** (`tasks/load_to_mongo.py`) — collects every batch's output, flattens it into
   one list of documents, and upserts each one into the configured MongoDB collection by `_id`
   (the Digimon's directory name), which the collection relies on for uniqueness. Existing
   documents are replaced in place; new ones are inserted.

4. **`reconcile_mongo`** (`tasks/reconcile_mongo.py`) — runs after the load completes and deletes
   any document in the collection whose `_id` wasn't produced by this run's scrape (e.g. a Digimon
   removed from `names.py` since the last run). Skips deletion entirely (with a warning) if the
   scrape produced zero documents, so a broken run can't wipe the collection.

Task order: `validate_references` → `scrape_digimon` (mapped) → `load_to_mongo` → `reconcile_mongo`.

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
| `MONGO_COLLECTION`   | `digimon`                   | Target collection name (upsert + reconcile) |

# Running

> Note: This was developed with Python 3.14.6 and Airflow 3.3.0. Deviating from this may create
> unexpected behavior.

The project is Dockerized to run standalone on a resource-constrained host (e.g. a small VM or a
single-board machine) — no Redis/Celery cluster, no internet access needed at container start
beyond the initial image build:

```bash
docker compose up --build
```
This starts:
- `mongo` — MongoDB on `localhost:27017`, matching the config defaults above so no extra setup is
  needed. Its WiredTiger cache is capped (`--wiredTigerCacheSizeGB 0.25`) since Mongo's default
  cache sizing (~50% of host RAM) is far too greedy for a constrained host, and the container is
  capped at 384MB / 0.5 CPU.
- `postgres` — the Airflow metadata database, capped at 256MB / 0.5 CPU. Used instead of the
  default SQLite backend: SQLite is a single writer, so every task/dag-run state transition across
  `standalone`'s separate processes (scheduler, dag-processor, api-server) serializes through it,
  which measurably added to Airflow's own memory/disk-I/O footprint. `airflow` waits for this
  service's healthcheck before starting.
- `airflow` — built from the project's own `Dockerfile` (`apache/airflow:3.3.0` plus the extra
  deps in `requirements-docker.txt`, with the DAG source baked in rather than bind-mounted, so the
  image is fully self-contained and portable). Runs as a single `airflow standalone` container
  (webserver, scheduler, and triggerer together) on `localhost:8080`, capped at 1.5GB / 1 CPU, with
  `MONGO_URI` and `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` pointed at their respective services.
  `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` is set to `false` so the DAG is immediately
  triggerable (Airflow pauses new DAGs by default otherwise), and DAG concurrency (parallelism,
  max active tasks/runs) is tuned to bound how many of the ~18 mapped scrape batches - each
  fanning out its own pool of concurrent requests - run at once. Peak memory measured ~95-98% of a
  1GB cap during a real run, so the limit was raised to 1.5GB for safety margin rather than
  dialing back the concurrency that got the run time down to ~2.5 minutes.

Because the DAG source is baked into the image at build time, editing `digivolution_dag.py`,
`tasks/`, or the data modules requires an image rebuild (`docker compose up --build`) to take
effect — it won't be picked up live.

On first boot, Airflow generates a random admin password. Grab it from the logs:
```bash
docker compose logs airflow | grep password
```
Then log in at `http://localhost:8080` and trigger `digivolution_scraper` (it also runs on its own
schedule — every 4 weeks). A full run scrapes every Digimon in `names.py` (~1300 pages);
end-to-end this currently takes a little over 2 minutes.

Local development in a virtualenv (for editing/testing task code without running Airflow itself):
```bash
source bin/activate
pip install -r requirements.txt
```

# Testing

```bash
source bin/activate
pytest
```

Runs the fast suite (`tests/unit/` and `tests/test_dag_structure.py`) — pure-function tests, task-level
tests against mocked HTTP responses (`requests_mock`) and an in-memory MongoDB (`mongomock`), and a
`DagBag`-based check that `digivolution_dag.py` imports cleanly with the expected tasks and
dependency edges. No network access or Docker required; this is what should run on every change.

A separate, slow end-to-end test drives the real `docker-compose` stack against the live
`digimon.net` site and asserts the resulting Mongo collection. It's opt-in since it takes a few
minutes and depends on the live site being reachable:

```bash
pytest --run-integration tests/integration/
```
