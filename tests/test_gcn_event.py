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


class _Event:
    def __init__(self, dateobs):
        self.dateobs = dateobs


def test_ambiguous_day_without_a_trigger_time_resolves_to_nothing():
    """Attaching a counterpart to the wrong event is worse than not attaching."""
    from datetime import datetime

    events = [_Event(datetime(2026, 6, 4, 8, 45)), _Event(datetime(2026, 6, 4, 20, 20))]
    assert gcn_event._pick(events, None) is None


def test_a_single_candidate_needs_no_trigger_time():
    from datetime import datetime

    only = _Event(datetime(2026, 6, 4, 20, 20))
    assert gcn_event._pick([only], None) is only


def test_trigger_time_separates_events_on_a_busy_day():
    from datetime import UTC, datetime

    fermi = _Event(datetime(2026, 6, 4, 8, 45))
    svom = _Event(datetime(2026, 6, 4, 20, 20))
    picked = gcn_event._pick([fermi, svom], datetime(2026, 6, 4, 20, 25, tzinfo=UTC))
    assert picked is svom
