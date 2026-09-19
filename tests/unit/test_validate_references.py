import logging

import pytest

from tasks.validate_references import (
    are_names_in_dict_valid,
    derive_inverse_relationship,
    should_skip_mapping,
    validate_references,
)


def test_derive_inverse_relationship_builds_reverse_mapping():
    result = derive_inverse_relationship({"agumon": ["greymon", "tyrannomon"], "gabumon": ["greymon"]})

    assert set(result["greymon"]) == {"agumon", "gabumon"}
    assert result["tyrannomon"] == ["agumon"]


def test_derive_inverse_relationship_handles_empty_input():
    assert derive_inverse_relationship({}) == {}


def test_are_names_in_dict_valid_detects_unknown_key(monkeypatch):
    monkeypatch.setattr("tasks.validate_references.digimon_names", ["agumon", "greymon"])

    assert are_names_in_dict_valid({"unknownmon": ["greymon"]}) is False


def test_are_names_in_dict_valid_detects_unknown_value(monkeypatch):
    monkeypatch.setattr("tasks.validate_references.digimon_names", ["agumon", "greymon"])

    assert are_names_in_dict_valid({"agumon": ["unknownmon"]}) is False


def test_are_names_in_dict_valid_detects_duplicates_in_list(monkeypatch):
    monkeypatch.setattr("tasks.validate_references.digimon_names", ["agumon", "greymon"])

    assert are_names_in_dict_valid({"agumon": ["greymon", "greymon"]}) is False


def test_are_names_in_dict_valid_accepts_clean_data(monkeypatch):
    monkeypatch.setattr("tasks.validate_references.digimon_names", ["agumon", "greymon"])

    assert are_names_in_dict_valid({"agumon": ["greymon"]}) is True


def test_should_skip_mapping_matches_known_unmapped_digimon():
    assert should_skip_mapping("burpmon") is True


def test_should_skip_mapping_rejects_ordinary_digimon():
    assert should_skip_mapping("agumon") is False


def test_validate_references_raises_on_duplicate_names(monkeypatch):
    monkeypatch.setattr("tasks.validate_references.digimon_names", ["agumon", "agumon"])

    with pytest.raises(ValueError):
        validate_references.function(logging.getLogger("test"))


def test_validate_references_passes_against_the_real_bootstrapping_data():
    # exercises the actual names.py/evolutions.py/modes.py shipped in the repo -
    # if this fails, the hand-maintained data itself is inconsistent
    validate_references.function(logging.getLogger("test"))
