import logging
from pathlib import Path

import pytest
import requests

from tasks.scrape_digimon import (
    _scrape_one,
    clean_attribute,
    clean_level,
    clean_name,
    derive_img_src,
    parse_special_moves,
    scrape_digimon,
    url_template,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
DETAIL_HTML = (FIXTURES_DIR / "digimon_detail.html").read_text()
NOT_FOUND_HTML = (FIXTURES_DIR / "digimon_not_found.html").read_text()

EMPTY_MAPPINGS = {"digimon_modes": {}, "previous_evolutions": {}, "next_evolutions": {}}


# --- pure functions ---


def test_clean_level_converts_roman_numerals():
    assert clean_level("Ⅰ") == "1"
    assert clean_level("Ⅱ") == "2"


def test_clean_level_strips_xros_wars_suffix():
    assert clean_level("Ⅰ(Xros Wars)") == "1"


def test_clean_name_converts_fullwidth_colon():
    assert clean_name("Angewomon：X") == "Angewomon:X"


def test_clean_attribute_empty_becomes_none():
    assert clean_attribute("") == "None"


def test_clean_attribute_passes_through_non_empty():
    assert clean_attribute("Vaccine") == "Vaccine"


def test_parse_special_moves_splits_and_strips():
    assert parse_special_moves("・Pepper Breath・Nova Blast") == ["Pepper Breath", "Nova Blast"]


def test_parse_special_moves_handles_empty_string():
    assert parse_special_moves("") == []


def test_derive_img_src_replaces_relative_prefix():
    assert derive_img_src("../cimages/digimon/agumon.jpg") == "https://digimon.net/cimages/digimon/agumon.jpg"


# --- _scrape_one against fixture HTML (no real network) ---


def test_scrape_one_parses_valid_page(requests_mock):
    requests_mock.get(f"{url_template}agumon", text=DETAIL_HTML)
    mappings = {
        "digimon_modes": {},
        "previous_evolutions": {"agumon": ["koromon"]},
        "next_evolutions": {"agumon": ["greymon"]},
    }

    with requests.Session() as session:
        result = _scrape_one(session, "agumon", mappings, logging.getLogger("test"))

    assert result["_id"] == "agumon"
    assert result["name"] == "Agumon"
    assert result["level"] == "Rookie"
    assert result["type"] == "Reptile"
    assert result["attribute"] == "Vaccine"
    assert result["moves"] == ["Pepper Breath", "Nova Blast"]
    assert result["img_src"] == "https://digimon.net/cimages/digimon/agumon.jpg"
    assert result["is_mode"] is False
    assert result["is_x_antibody"] is False
    assert result["previous_digivolutions"] == ["koromon"]
    assert result["next_digivolutions"] == ["greymon"]


def test_scrape_one_raises_when_content_missing(requests_mock):
    requests_mock.get(f"{url_template}invalidmon", text=NOT_FOUND_HTML)

    with requests.Session() as session, pytest.raises(ValueError, match="Couldn't find data"):
        _scrape_one(session, "invalidmon", EMPTY_MAPPINGS, logging.getLogger("test"))


def test_scrape_one_raises_on_http_error(requests_mock):
    requests_mock.get(f"{url_template}brokenmon", status_code=500)

    with requests.Session() as session, pytest.raises(requests.exceptions.RequestException):
        _scrape_one(session, "brokenmon", EMPTY_MAPPINGS, logging.getLogger("test"))


# --- scrape_digimon (the batch task) ---


def test_scrape_digimon_returns_all_results_on_success(requests_mock):
    requests_mock.get(f"{url_template}agumon", text=DETAIL_HTML)
    requests_mock.get(f"{url_template}gabumon", text=DETAIL_HTML)

    results = scrape_digimon.function(["agumon", "gabumon"], EMPTY_MAPPINGS, logging.getLogger("test"))

    assert len(results) == 2
    assert {r["_id"] for r in results} == {"agumon", "gabumon"}


def test_scrape_digimon_isolates_a_single_failure_then_raises(requests_mock):
    requests_mock.get(f"{url_template}agumon", text=DETAIL_HTML)
    requests_mock.get(f"{url_template}badmon", text=NOT_FOUND_HTML)

    with pytest.raises(ValueError, match=r"Failed to scrape 1 Digimon.*badmon"):
        scrape_digimon.function(["agumon", "badmon"], EMPTY_MAPPINGS, logging.getLogger("test"))
