# pyright: reportOptionalSubscript=false, reportOptionalMemberAccess=false
import argparse
import itertools
import json
import sys
from time import sleep

import requests
from bs4 import BeautifulSoup
from evolutions import next_evolutions
from modes import digimon_modes, known_mode_variants
from names import digimon_names

output_path = "../data/digimon.json"
url_template = "https://digimon.net/reference_en/detail.php?directory_name="  # url param is the CASE SENSITIVE name of the digimon
img_domain = "https://digimon.net/"

# Tags to look up
parent_tag = "p-ref"  # encompassing class tag
en_name_tag = "c-titleSet__main"  # localized English name
info_tag = "p-ref__info"  # section that has details like level, type, attribute, and special move(s)
profile_tag = "p-ref__txt"  # description of the Digimon

# There is a set of digimon that don't have any evolution mappings to them (not even TCG), for whatever reason.
skipped = {"burpmon", "yggdrasill7d6", "yoxtuyoxtumon", "heliosboamon"}


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
    return s.replace("\u2160", "1").replace("\u2161", "2").replace("(Xros Wars)", "")


def clean_name(s: str) -> str:
    return s.replace("\uff1a", ":")


def clean_attribute(s: str) -> str:
    if len(s) == 0:
        return "None"
    return s


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


def should_skip_mapping(name: str) -> bool:
    return name in skipped
    # return (
    #     name.endswith(("_x", "-x", "-xwars"))
    #     or name.startswith("shoutmon")
    #     or name in skipped
    # )


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


def validate_references():
    # there should not be duplicate names.
    # if there are duplicates, that has to be addressed before continuing
    # to scrape the data.
    name_set = set(digimon_names)
    diff = abs(len(digimon_names) - len(name_set))
    if diff != 0:
        print(f"Found {diff} duplicates in the data. Cannot proceed.")
        print([x for x in digimon_names if digimon_names.count(x) > 1])
        sys.exit(1)

    # for every mode, that name should be the directory name/ID, not the localized name
    print("Checking modes...")
    for known in known_mode_variants:
        if known not in digimon_names:
            print(f"ERROR: {known} is not a valid Digimon.")
            sys.exit(1)
    if not are_names_in_dict_valid(digimon_modes):
        print("At least one Digimon mode is invalid.")
        sys.exit(1)

    # for every evolution chain, each name should be the directory name/ID, not the localized name
    previous_evolutions = derive_inverse_relationship(next_evolutions)
    print("Checking next evolutions...")
    if not are_names_in_dict_valid(next_evolutions):
        print("At least one next evolution chain is invalid.")
        sys.exit(1)
    print("Checking previous evolutions...")
    if not are_names_in_dict_valid(previous_evolutions):
        print("At least one previous evolution chain is invalid.")
        sys.exit(1)

    # TODO: toggle as we go. check each name that exists in the digivolution maps,
    #       and compute the set difference between that and the total set of names
    #       in order to determine how many digimon still need to be mapped
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
    print(
        f"There are {len(unmapped)} Digimon out of {len(digimon_names)} that don't have evolution mappings yet."
    )
    unmapped = sorted([name for name in unmapped if not should_skip_mapping(name)])
    print(f"There are {len(unmapped)} Digimon that SHOULD have mappings.")

    if len(unmapped) > 0:
        print("UNMATCHED DIGIMON MUST BE ADDRESSED.")
        for batch in itertools.batched(unmapped, 8):
            print("\t", list(batch))
        print("Exiting early to avoid compute...")
        sys.exit(0)


def main():
    parser = argparse.ArgumentParser(prog="DigimonQL Scraper", description="Scrape Digimon Encyclopedia to populate local JSON file")
    parser.add_argument("--names", action="extend", nargs="+", help="variable length list of digimon IDs to specifically target for scraping")
    args = parser.parse_args()

    full_set = True
    input_names = digimon_names
    if args.names is not None:
        full_set = False
        input_names = args.names

    validate_references()  # ensure the bootstrapping data is all valid before beginning to scrape

    # derive mappings
    previous_evolutions = derive_inverse_relationship(next_evolutions)
    digimon_modes.update(derive_inverse_relationship(digimon_modes))

    data = []
    failures = []
    for name in input_names:
        digimon_url = f"{url_template}{name}"
        print(f"Checking {digimon_url}...")
        r = requests.get(digimon_url)
        soup = BeautifulSoup(r.text, features="html.parser")

        digimon = soup.find(class_=parent_tag)
        if digimon is None:
            print(f"Couldn't find data for {name} at {digimon_url}")
            print("Skipping to next digimon")
            failures.append(name)
            continue

        english_name = clean_name(digimon.find(class_=en_name_tag).text)
        img_url = derive_img_src(digimon.find("img")["src"])  # pyright:ignore

        info = digimon.find(class_=info_tag)
        if info is None:
            print(f"Couldn't find {info_tag} for {name}. Skipping.")
            failures.append(name)
            continue
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
        result["name"] = english_name  # TODO: how should this handle localized names?
        result["level"] = digimon_level
        result["type"] = digimon_type
        result["attribute"] = clean_attribute(digimon_attr)
        result["moves"] = digimon_moves
        result["img_src"] = img_url
        result["background"] = digimon.find(class_=profile_tag).text.strip()
        result["is_mode"] = name in known_mode_variants or " Mode" in english_name
        result["is_x_antibody"] = "(X Antibody)" in english_name

        if name in digimon_modes:
            result["modes"] = digimon_modes[name]

        # don't assume that the digimon exists in the map
        if name in previous_evolutions:
            result["previous_digivolutions"] = previous_evolutions[name]
        if name in next_evolutions:
            result["next_digivolutions"] = next_evolutions[name]

        data.append(result)

        sleep(0.03)  # wait for rate-limiting

    # after iterating over all of the digimon, check the failures (if any).
    # if there are failures, flag it to be addressed and exit early
    if len(failures) > 0:
        print(f"Had an issue finding {len(failures)} digimon. Triage these:")
        for name in failures:
            print(f"\t{name}")
        sys.exit(1)

    # if there are no failures, write the data out as JSON to be used as the backing data for the database
    # note: if full_set is False, then we want to merge with the existing data set
    if full_set:
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)  # pyright:ignore
            print(f"Successfully wrote out {len(data)} digimon scraped to {output_path}")
    else:
        with open(output_path, "r") as f:
            # read the dataset first
            current_data = json.load(f)
        # need to account for collisions in the dataset with the input, so can't do simple .extend() operation
        data.extend([d for d in current_data if d["_id"] not in input_names])
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)
            print(f"Successfully wrote out {len(data)} digimon scraped to {output_path}")


if __name__ == "__main__":
    main()
