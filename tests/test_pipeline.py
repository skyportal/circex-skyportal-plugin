"""Pure helpers. The write path is exercised in fritz's in-container tests."""

import pytest

import pipeline


def test_mjd_to_iso_round_trips_a_known_epoch():
    assert pipeline.mjd_to_iso(51544.0).startswith("2000-01-01")


def test_build_extractor_rejects_an_unknown_kind(cfg):
    cfg["extractor"]["kind"] = "gpt"
    with pytest.raises(ValueError, match="unknown extractor kind"):
        pipeline.build_extractor(cfg)


class _Point:
    def __init__(self, mjd, filter="ztfg"):
        self.mjd = mjd
        self.filter = filter


def test_detection_window_brackets_the_photometry():
    start, end = pipeline.detection_window([_Point(61195.0), _Point(61197.0)], pad_days=1.0)
    assert start.startswith("2026-06-03")
    assert end.startswith("2026-06-07")


def test_detection_window_is_none_without_photometry():
    assert pipeline.detection_window([]) is None


class _Event:
    def __init__(self, name):
        self.event_name = name


class _Extraction:
    def __init__(self, name):
        self.event = _Event(name)


class _Actions:
    def __init__(self, names):
        self.extractions = [_Extraction(n) for n in names]


def test_designations_sort_ahead_of_counterpart_names():
    """A counterpart name never resolves an event, so it must not be tried first."""
    names = pipeline.event_names(_Actions([["AT2017gfo", "GW170817"]]))
    assert names == ["GW170817", "AT2017gfo"]


def test_event_names_are_deduplicated():
    assert pipeline.event_names(_Actions(["GRB 260604C", "GRB 260604C"])) == ["GRB 260604C"]


def test_prepare_runs_without_a_session(monkeypatch):
    """The slow half must not need a session; that is what keeps it out of the
    transaction Postgres would time out."""
    import pipeline

    records = [{"circularId": 1, "body": "b", "subject": "s"}]
    monkeypatch.setattr("circex.bot.aggregate.gather_by_xref", lambda *a, **k: records)
    monkeypatch.setattr("circex.bot.aggregate.aggregate_event", lambda *a, **k: "actions")
    got_records, actions = pipeline.prepare_circular(
        {"circularId": 1}, extractor=None, fetch=None, cfg={}
    )
    assert got_records == records
    assert actions == "actions"
