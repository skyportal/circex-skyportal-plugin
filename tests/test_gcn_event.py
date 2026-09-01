"""Event resolution and the writes that attach an extraction to a GcnEvent."""

from datetime import UTC, date, datetime

import pytest

import gcn_event
from tests.conftest import EVENT, FakeSkyPortal


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("GRB 260604C", date(2026, 6, 4)),
        ("GRB260604C", date(2026, 6, 4)),
        ("GRB 990123", date(1999, 1, 23)),  # two-digit year pivots to last century
        ("GW170817", date(2017, 8, 17)),
        ("S190425z", date(2019, 4, 25)),
        ("EP240315a", date(2024, 3, 15)),
        ("SVOM 250101A", date(2025, 1, 1)),
        ("AT2017gfo", None),  # a counterpart name, not an event designation
        ("GRB 999999", None),  # six digits that aren't a date
        ("", None),
    ],
)
def test_parse_designation(name, expected):
    assert gcn_event.parse_designation(name) == expected


def test_resolves_by_designation_when_no_alias_matches(client):
    match = gcn_event.resolve_event(client, names=["GRB 260604C"])
    assert match is not None
    assert match.dateobs == EVENT["dateobs"]
    assert match.matched_by == "designation:GRB 260604C"


def test_resolves_by_alias_once_the_alias_has_been_written(client):
    gcn_event.write_event_bindings(
        client,
        gcn_event.EventMatch(dateobs=EVENT["dateobs"]),
        names=["GRB 260604C"],
        obj_id=None,
        comment=None,
    )
    match = gcn_event.resolve_event(client, names=["GRB 260604C"])
    assert match.matched_by == "alias:GRB 260604C"


def test_alias_lookup_tries_the_spaceless_spelling(client):
    """Circulars write "GRB 260604C"; notices and TACH write "GRB260604C"."""
    client.events[0]["aliases"] = ["GRB260604C"]
    match = gcn_event.resolve_event(client, names=["GRB 260604C"])
    assert match.matched_by == "alias:GRB260604C"


def test_lvk_superevent_resolves_against_the_notice_alias(client):
    client.events[0]["aliases"] = ["LVC#S260604a"]
    match = gcn_event.resolve_event(client, names=["S260604a"])
    assert match.matched_by == "alias:S260604a"


def test_unknown_event_resolves_to_none(client):
    assert gcn_event.resolve_event(client, names=["GRB 010101A"]) is None


def test_nearest_event_wins_when_a_day_holds_several(client):
    client.events.append({"dateobs": "2026-06-04T02:00:00", "aliases": [], "localizations": []})
    match = gcn_event.resolve_event(
        client,
        names=["GRB 260604C"],
        trigger_time=datetime(2026, 6, 4, 20, 0, tzinfo=UTC),
    )
    assert match.dateobs == "2026-06-04T20:21:59"


def test_trigger_rung_resolves_without_a_parseable_name(client):
    match = gcn_event.resolve_event(
        client,
        names=["AT2026abc"],
        trigger_time=datetime(2026, 6, 4, 20, 0, tzinfo=UTC),
        order=["alias", "designation", "trigger"],
    )
    assert match.matched_by == "trigger"


def test_confirm_in_gcn_needs_a_localization():
    """An event with no skymap yet cannot take a source association."""
    client = FakeSkyPortal()
    match = gcn_event.EventMatch(dateobs=EVENT["dateobs"], localizations=[])
    gcn_event.write_event_bindings(
        client,
        match,
        names=[],
        obj_id="GRB260604C",
        comment=None,
        detection_window=("2026-06-04", "2026-06-06"),
    )
    assert client.paths("sources_in_gcn") == []


def test_confirm_in_gcn_posts_against_the_newest_localization(client):
    match = gcn_event.EventMatch(dateobs=EVENT["dateobs"], localizations=EVENT["localizations"])
    gcn_event.write_event_bindings(
        client,
        match,
        names=[],
        obj_id="GRB260604C",
        comment=None,
        detection_window=("2026-06-04", "2026-06-06"),
    )
    (write,) = client.paths("sources_in_gcn")
    assert write["payload"]["localization_name"] == "svom_eclairs_1"
    assert write["payload"]["confirmed"] is True
    assert write["payload"]["source_id"] == "GRB260604C"


def test_write_channels_can_be_switched_off(client):
    gcn_event.write_event_bindings(
        client,
        gcn_event.EventMatch(dateobs=EVENT["dateobs"], localizations=EVENT["localizations"]),
        names=["GRB 260604C"],
        obj_id="GRB260604C",
        comment="hello",
        detection_window=("2026-06-04", "2026-06-06"),
        writes={"alias": False, "confirm_in_gcn": False, "comment": False, "tag": False},
    )
    assert client.plan == []


def test_an_alias_already_on_the_event_is_not_reposted(client):
    match = gcn_event.EventMatch(dateobs=EVENT["dateobs"], aliases=["GRB 260604C"])
    gcn_event.write_event_bindings(client, match, names=["GRB 260604C"], obj_id=None, comment=None)
    assert client.paths("alias") == []
