"""Synthesized cone localizations, and the event comment posted as the bot."""

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

import gcn_event
import pipeline


@pytest.mark.parametrize(
    ("stated", "expected"),
    [
        (0.5, 0.5),  # a stated radius is used as written
        ([0.4, 0.2, 30.0], 0.4),  # an ellipse contributes its semi-major axis
        (None, gcn_event.DEFAULT_LOCALIZATION_ERROR_DEG),
        (0.0, gcn_event.DEFAULT_LOCALIZATION_ERROR_DEG),
        (0.0001, gcn_event.MIN_LOCALIZATION_ERROR_DEG),  # floor
        (90.0, None),  # too large to represent as an ellipse; refused
    ],
)
def test_cone_radius_is_clamped(stated, expected):
    assert gcn_event.cone_radius(stated) == expected


@dataclass
class FakeWriter:
    live: bool = False
    user_id: int = 1
    plan: list[tuple[str, dict]] = field(default_factory=list)

    def _record(self, kind, payload):
        self.plan.append((kind, payload))


def test_an_event_that_already_has_a_skymap_is_left_alone():
    match = gcn_event.EventMatch(dateobs="2026-06-04T00:00:00", localizations=["bayestar"])
    writer = FakeWriter()
    name = asyncio.run(gcn_event.ensure_localization(None, writer, match, 10.0, -20.0, None))
    assert name == "bayestar"
    assert writer.plan == []


def test_a_circular_only_event_plans_a_cone():
    """Konus-Wind and similar arrive with no Notice, so no localization exists."""
    match = gcn_event.EventMatch(dateobs="2026-06-04T00:00:00", localizations=[])
    writer = FakeWriter()
    asyncio.run(gcn_event.ensure_localization(None, writer, match, 10.0, -20.0, 0.3))
    assert [kind for kind, _ in writer.plan] == ["localization"]
    assert writer.plan[0][1]["error"] == 0.3


def test_no_position_means_no_cone():
    match = gcn_event.EventMatch(dateobs="2026-06-04T00:00:00", localizations=[])
    writer = FakeWriter()
    assert asyncio.run(gcn_event.ensure_localization(None, writer, match, None, None, None)) is None
    assert writer.plan == []


# --- the event comment ------------------------------------------------------


@dataclass
class FakeRow:
    frequency_ghz: float | None = None
    flux_density: float | None = None
    flux_density_error: float | None = None
    limiting_flux_density: float | None = None
    flux_density_unit: str | None = None
    limiting_mag_sigma: float | None = None
    filter: str | None = None
    bandpass: str | None = None
    mag: float | None = None
    mag_error: float | None = None
    limiting_mag: float | None = None
    obs_time: str | None = None
    telescope: str | None = None


@dataclass
class FakeMeta:
    extractor: str = "regex-v1"
    notes: list[str] = field(default_factory=list)


@dataclass
class FakeExtraction:
    photometry: list[Any] = field(default_factory=list)
    extraction_meta: FakeMeta = field(default_factory=FakeMeta)
    classification: Any = None


@dataclass
class FakeSource:
    id: str = "GRB230307A"
    ra: float = 45.12
    dec: float = -75.38


@dataclass
class FakeActions:
    source: Any = None
    photometry: list[Any] = field(default_factory=list)
    redshift: Any = None
    extractions: tuple = ()
    skipped_reasons: tuple = ()


@dataclass
class FakePoint:
    filter: str
    mjd: float = 60015.1


def test_the_comment_reports_the_circular_and_the_writes():
    rows = [
        FakeRow(
            frequency_ghz=9.0,
            flux_density=120.0,
            flux_density_error=30.0,
            flux_density_unit="uJy",
            telescope="ATCA",
            obs_time="2023-03-12T02:30:00Z",
        ),
        FakeRow(
            frequency_ghz=5.5,
            limiting_flux_density=90.0,
            limiting_mag_sigma=3.0,
            flux_density_unit="uJy",
            telescope="ATCA",
        ),
    ]
    actions = FakeActions(
        source=FakeSource(),
        photometry=[FakePoint("radio-10GHz"), FakePoint("radio-6GHz")],
        extractions=(FakeExtraction(photometry=rows),),
        skipped_reasons=("no bandpass",),
    )
    text = pipeline.build_comment(
        actions, [{"circularId": 33475, "subject": "GRB 230307A: ATCA radio detection"}]
    )
    # the circular itself
    assert "GCN 33475" in text
    assert "GRB 230307A: ATCA radio detection" in text
    assert "https://gcn.nasa.gov/circulars/33475" in text
    # what it reported, in the units the circular used
    assert "9 GHz: 120 +/- 30 uJy" in text
    assert "5.5 GHz: < 90 uJy (3 sigma)" in text
    assert "ATCA" in text
    # what was written, and what was not
    assert "GRB230307A" in text
    assert "2 photometry point(s)" in text
    assert "1 photometry row(s): no bandpass" in text


def test_optical_rows_are_reported_as_magnitudes():
    rows = [
        FakeRow(filter="r", mag=18.42, mag_error=0.05),
        FakeRow(filter="g", limiting_mag=22.5, limiting_mag_sigma=3.0),
    ]
    actions = FakeActions(extractions=(FakeExtraction(photometry=rows),))
    text = pipeline.build_comment(actions, [{"circularId": 1, "subject": "s"}])
    assert "r = 18.42 +/- 0.05" in text
    assert "g > 22.5 (3 sigma)" in text


def test_a_circular_with_nothing_postable_says_so():
    actions = FakeActions(extractions=(FakeExtraction(),))
    text = pipeline.build_comment(actions, [{"circularId": 1, "subject": "s"}])
    assert "nothing postable" in text


@pytest.mark.parametrize(
    ("stated", "kind"),
    [
        ([1.225, 0.158], "ellipse"),  # an IPN box, long and thin
        (0.5, "cone"),  # a stated radius
        (None, "cone"),  # nothing stated; the default radius
        ([2.0], "cone"),  # only one axis is not an ellipse
    ],
)
def test_the_region_shape_follows_what_the_circular_states(stated, kind):
    assert gcn_event.localization_shape(stated)["kind"] == kind


def test_an_ipn_box_keeps_both_axes():
    """A circle enclosing a 2.45 x 0.32 deg box would be far larger than the box."""
    shape = gcn_event.localization_shape([1.225, 0.158])
    assert (shape["amaj"], shape["amin"]) == (1.225, 0.158)


def test_an_ipn_annulus_is_refused_rather_than_shrunk():
    """GCN 21735's strip is 99.7 deg across; narrowing it would invent precision."""
    assert gcn_event.localization_shape([49.85, 0.64]) is None
