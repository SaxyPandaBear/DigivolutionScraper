import itertools
import logging
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag

from evolutions import next_evolutions
from modes import digimon_modes
from names import digimon_names
from tasks.load_to_mongo import load_to_mongo
from tasks.reconcile_mongo import reconcile_mongo
from tasks.scrape_digimon import scrape_digimon
from tasks.validate_references import derive_inverse_relationship, validate_references

logger = logging.getLogger(__name__)

# these mappings are pure functions of static, hardcoded data (no I/O), so it's
# cheap to derive them once at DAG-parse time and hand the same dict to every
# mapped scrape task instance instead of recomputing it per-task.
previous_evolutions = derive_inverse_relationship(next_evolutions)
digimon_modes_with_inverse = dict(digimon_modes)
digimon_modes_with_inverse.update(derive_inverse_relationship(digimon_modes))
evolution_mappings = {
    "next_evolutions": next_evolutions,
    "previous_evolutions": previous_evolutions,
    "digimon_modes": digimon_modes_with_inverse,
}

# fan out in batches rather than one mapped task per Digimon, to keep the
# number of task instances (and scheduler overhead) reasonable.
BATCH_SIZE = 25
digimon_name_batches = [list(batch) for batch in itertools.batched(digimon_names, BATCH_SIZE)]


@dag(
    "digivolution_scraper",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=30),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="A simple tutorial DAG",
    schedule=timedelta(weeks=4),
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    tags=["digimon"],
)
def populate_digivolutions():
    t1 = validate_references(logger)
    scraped_digimon = scrape_digimon.partial(mappings=evolution_mappings, logger=logger).expand(
        names=digimon_name_batches
    )
    loaded = load_to_mongo(scraped_digimon, logger)
    reconciled = reconcile_mongo(scraped_digimon, logger)

    t1 >> scraped_digimon >> loaded >> reconciled


populate_digivolutions()
