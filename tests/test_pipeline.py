"""The circular -> GcnEvent path, driven over the vendored GRB 260604C flurry."""

from collections import Counter

import pytest

import pipeline
from tests.conftest import EVENT, requires_extractions


@pytest.fixture
def run(cfg, client, flurry):
    """Replay the whole flurry; return (results, client, state)."""
    from circex.consume.sources import dir_fetch, replay_dir_records

    extractor = pipeline.build_extractor(cfg)
    fetch = dir_fetch(flurry)
    state = pipeline.SessionState()
    results = [
        pipeline.process_circular(
            record, extractor=extractor, client=client, fetch=fetch, cfg=cfg, state=state
        )
        for record in replay_dir_records(flurry)
    ]
    return results, client, state


def test_mjd_to_iso_round_trips_a_known_epoch():
    assert pipeline.mjd_to_iso(51544.0).startswith("2000-01-01")


def test_build_extractor_rejects_an_unknown_kind(cfg):
    cfg["extractor"]["kind"] = "gpt"
    with pytest.raises(ValueError, match="unknown extractor kind"):
        pipeline.build_extractor(cfg)


def test_detection_window_brackets_the_photometry():
    class Point:
        def __init__(self, mjd):
            self.mjd = mjd

    start, end = pipeline.detection_window([Point(61195.0), Point(61197.0)], pad_days=1.0)
    assert start.startswith("2026-06-03")
    assert end.startswith("2026-06-07")


def test_detection_window_is_none_without_photometry():
    assert pipeline.detection_window([]) is None


@requires_extractions
def test_flurry_binds_to_one_event(run):
    results, _, _ = run
    posted = [r for r in results if r.status == "posted"]
    assert posted, "no circular resolved to a GcnEvent"
    assert {r.dateobs for r in posted} == {EVENT["dateobs"]}
    assert {r.obj_id for r in posted} == {"GRB260604C"}


@requires_extractions
def test_photometry_accumulates_and_deduplicates(run):
    results, client, _ = run
    posted_points = len(client.paths("/photometry"))
    assert posted_points > 0
    # Every point posted exactly once across the flurry.
    assert posted_points == sum(r.photometry_posted for r in results)
    assert sum(r.photometry_skipped for r in results) > 0, "flurry should overlap"


@requires_extractions
def test_repeated_circulars_do_not_repost_the_alias(run):
    _, client, _ = run
    aliases = [w["payload"]["alias"] for w in client.paths("/alias")]
    assert aliases == list(dict.fromkeys(aliases)), "an alias was posted twice"


@requires_extractions
def test_the_event_is_tagged_once(run):
    _, client, _ = run
    assert len(client.paths("/tags")) <= 1


@requires_extractions
def test_the_source_is_not_reupserted_at_an_unchanged_position(run):
    results, client, _ = run
    upserts = client.paths("/sources")
    upserts = [w for w in upserts if w["path"] == "/sources"]
    positions = {(w["payload"]["ra"], w["payload"]["dec"]) for w in upserts}
    assert len(upserts) == len(positions), "same position upserted twice"


@requires_extractions
def test_photometry_carries_the_event_and_its_circular(run):
    _, client, _ = run
    for write in client.paths("/photometry"):
        altdata = write["payload"]["altdata"]
        assert altdata["gcn_dateobs"] == EVENT["dateobs"]
        assert "circex_circular_id" in altdata


@requires_extractions
def test_an_unknown_event_parks_rather_than_posting(cfg, flurry):
    from circex.consume.sources import dir_fetch, replay_dir_records

    from tests.conftest import FakeSkyPortal

    client = FakeSkyPortal(events=[])  # SkyPortal knows of no events at all
    extractor = pipeline.build_extractor(cfg)
    results = [
        pipeline.process_circular(
            record,
            extractor=extractor,
            client=client,
            fetch=dir_fetch(flurry),
            cfg=cfg,
            state=pipeline.SessionState(),
        )
        for record in replay_dir_records(flurry)
    ]
    assert Counter(r.status for r in results)["unresolved-event"] > 0
    assert client.plan == [], "nothing may be written for an unresolved event"


@requires_extractions
def test_writes_switched_off_produce_no_requests(cfg, client, flurry):
    from circex.consume.sources import dir_fetch, replay_dir_records

    cfg["writes"] = dict.fromkeys(["source", "alias", "confirm_in_gcn", "comment", "tag"], False)
    extractor = pipeline.build_extractor(cfg)
    state = pipeline.SessionState()
    for record in replay_dir_records(flurry):
        pipeline.process_circular(
            record,
            extractor=extractor,
            client=client,
            fetch=dir_fetch(flurry),
            cfg=cfg,
            state=state,
        )
    assert client.plan == []
