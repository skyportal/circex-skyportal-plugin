"""Designation decoding and alias matching. Resolution against real events is
covered by the in-container tests in fritz."""

import datetime

import pytest

import gcn_event


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("GRB 260604C", datetime.date(2026, 6, 4)),
        ("GRB260604C", datetime.date(2026, 6, 4)),
        ("GRB 990123", datetime.date(1999, 1, 23)),  # two-digit year is last century
        ("GW170817", datetime.date(2017, 8, 17)),
        ("S190425z", datetime.date(2019, 4, 25)),  # LVK superevent
        ("EP240315a", datetime.date(2024, 3, 15)),
        ("SVOM 250101A", datetime.date(2025, 1, 1)),
        ("IC220624A", datetime.date(2022, 6, 24)),
    ],
)
def test_designation_date_is_decoded(name, expected):
    assert gcn_event.parse_designation(name) == expected


@pytest.mark.parametrize(
    "name",
    [
        "AT2017gfo",  # a counterpart name, not an event designation
        "GRB 999999",  # six digits that are not a date
        "GRB 261340A",  # month 13
        "",
    ],
)
def test_undecodable_designations_return_none(name):
    assert gcn_event.parse_designation(name) is None


@pytest.mark.parametrize(
    ("aliases", "name", "found"),
    [
        (["LVC#S190814bv"], "S190814bv", True),  # notice alias carries a prefix
        (["GRB260604C"], "GRB 260604C", True),  # circulars write the space
        (["grb260604c"], "GRB 260604C", True),  # TACH uppercases
        (["GRB 260604C"], "GRB260604C", True),
        (["FERMI#bn180116026"], "GRB 260604C", False),
        ([], "GRB 260604C", False),
    ],
)
def test_alias_matching_ignores_case_and_spaces(aliases, name, found):
    assert gcn_event._alias_present(aliases, name) is found
