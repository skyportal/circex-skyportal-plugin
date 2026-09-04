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


def test_a_retraction_marks_earlier_photometry_unreliable():
    """GCN 45503 retracted the counterpart GCN 45501 reported.

    The rows are kept and flagged: a reader who saw the earlier magnitude needs
    to find it marked rather than missing.
    """
    import skyportal_db

    writer = skyportal_db.SkyPortalWriter(live=False)
    calls = []
    writer._record = lambda op, payload: calls.append((op, payload))

    import asyncio

    n = asyncio.run(
        writer.reject_photometry(None, "GRB260903A", "Counterpart retracted by GCN 45503.")
    )
    assert n == 0  # dry run writes nothing
    assert calls == [
        (
            "reject_photometry",
            {"obj_id": "GRB260903A", "explanation": "Counterpart retracted by GCN 45503."},
        )
    ]


def test_prepare_runs_on_the_calling_thread():
    """The LLM cache holds a SQLite connection bound to one thread.

    Running prepare_circular on a worker thread raised
    "SQLite objects created in a thread can only be used in that same thread"
    on every circular, so the consumer must call it directly.
    """
    import inspect

    import main

    source = inspect.getsource(main.handle_record)
    assert "prepare_circular" in source
    assert "to_thread" not in source


def test_prepare_passes_a_trigger_time(monkeypatch):
    """Relative epochs ("2.30 hr after the trigger") need one.

    Without it aggregate_event defaults to None and every such row is dropped
    for having no observation time.
    """
    import pipeline

    seen = {}

    def _aggregate(records, extractor, **kwargs):
        seen.update(kwargs)
        return "actions"

    monkeypatch.setattr(
        "circex.bot.aggregate.gather_by_xref",
        lambda *a, **k: [
            {"circularId": 1, "createdOn": 1_756_000_000_000, "body": "b", "subject": "s"}
        ],
    )
    monkeypatch.setattr("circex.bot.aggregate.aggregate_event", _aggregate)
    pipeline.prepare_circular({"circularId": 1}, extractor=None, fetch=None, cfg={})
    assert seen.get("trigger_time") is not None


def test_prepare_accepts_a_trigger_override(monkeypatch):
    """The event's dateobs is the burst time; the first circular's timestamp
    trails it by however long the observers took to write.

    GCN 45505's three epochs landed 42 minutes late on that proxy alone.
    """
    import datetime

    import pipeline

    seen = {}
    monkeypatch.setattr(
        "circex.bot.aggregate.gather_by_xref",
        lambda *a, **k: [
            {"circularId": 1, "createdOn": 1_756_000_000_000, "body": "b", "subject": "s"}
        ],
    )
    monkeypatch.setattr(
        "circex.bot.aggregate.aggregate_event",
        lambda records, extractor, **kw: seen.update(kw) or "actions",
    )
    burst = datetime.datetime(2026, 9, 3, 12, 36, 47, tzinfo=datetime.UTC)
    pipeline.prepare_circular(
        {"circularId": 1}, extractor=None, fetch=None, cfg={}, trigger_time=burst
    )
    assert seen["trigger_time"] == burst
