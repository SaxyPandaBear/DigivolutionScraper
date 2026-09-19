import itertools
import logging

from airflow.sdk import task

from evolutions import next_evolutions
from modes import digimon_modes, known_mode_variants
from names import digimon_names

# There is a set of digimon that don't have any evolution mappings to them (not even TCG), for whatever reason.
skipped = {"burpmon", "yggdrasill7d6", "yoxtuyoxtumon"}

def should_skip_mapping(name: str) -> bool:
    return name in skipped

def are_names_in_dict_valid(d: dict[str, list[str]]) -> bool:
    error_found = False
    for k, v in d.items():
        if k not in digimon_names:
            print(f"ERROR: {k} is not a valid Digimon.")
            error_found = True
        # check for duplicates in the list
        dupes = set(v)
        if len(dupes) != len(v):
            print(f"Duplicates found in list {v} corresponding to {k}")
            error_found = True  # at least one duplicate in the list
        for v1 in v:
            if v1 not in digimon_names:
                print(f"ERROR: {v1} is not a valid Digimon.")
                error_found = True
    return not error_found

# logically, we can iterate over the unidirectional digivolutions and
# derive the link going backwards as its own separate set. this is to cut
# down on how much time it takes to handwrite the mappings.
def derive_inverse_relationship(mappings: dict[str, list[str]]) -> dict[str, list[str]]:
    temp = {}

    for k, v in mappings.items():
        for digimon in v:
            if digimon not in temp:
                # place a new entry
                temp[digimon] = {k}
            else:
                # already exists, add k to the set
                temp[digimon].add(k)

    # coerece back to a list
    result = {}
    for k, v in temp.items():
        result[k] = list(v)
    return result

@task
def validate_references(logger: logging.Logger):
    # there should not be duplicate names.
    # if there are duplicates, that has to be addressed before continuing
    # to scrape the data.
    name_set = set(digimon_names)
    diff = abs(len(digimon_names) - len(name_set))
    if diff != 0:
        logger.error(f"Found {diff} duplicates in the data. Cannot proceed.")
        raise ValueError

    # for every mode, that name should be the directory name/ID, not the localized name
    logger.info("Checking modes...")
    for known in known_mode_variants:
        if known not in digimon_names:
            logger.error(f"'{known}' is not a valid Digimon.")
            raise ValueError
    if not are_names_in_dict_valid(digimon_modes):
        logger.error("At least one Digimon mode is invalid.")
        raise ValueError

    # for every evolution chain, each name should be the directory name/ID, not the localized name
    previous_evolutions = derive_inverse_relationship(next_evolutions)
    logger.info("Checking next evolutions...")
    if not are_names_in_dict_valid(next_evolutions):
        logger.error("At least one next evolution chain is invalid.")
        raise ValueError
    logger.info("Checking previous evolutions...")
    if not are_names_in_dict_valid(previous_evolutions):
        logger.error("At least one previous evolution chain is invalid.")
        raise ValueError

    universe = set(digimon_names)
    mapped = []
    mapped.extend(previous_evolutions.keys())
    for v in previous_evolutions.values():
        mapped.extend(v)
    mapped.extend(next_evolutions.keys())
    for v in next_evolutions.values():
        mapped.extend(v)
    mapped.extend(digimon_modes.keys())
    for v in digimon_modes.values():
        mapped.extend(v)
    mapped = set(mapped)

    # compute the set difference. note that if we got this far, there should not
    # be any value in `mapped` that doesn't exist in universe.
    unmapped = universe.difference(mapped)
    logger.info(
        f"There are {len(unmapped)} Digimon out of {len(digimon_names)} that don't have evolution mappings yet."
    )
    unmapped = sorted([name for name in unmapped if not should_skip_mapping(name)])
    logger.info(f"There are {len(unmapped)} Digimon that SHOULD have mappings.")

    if len(unmapped) > 0:
        logger.warning("UNMATCHED DIGIMON MUST BE ADDRESSED.")
        for batch in itertools.batched(unmapped, 8, strict=False):
            logger.warning(f"\t{list(batch)}")
        logger.warning("Exiting early to avoid compute...")
        raise ValueError

    # succeeded
    logger.info("Successfully validated bootstrapping data")
