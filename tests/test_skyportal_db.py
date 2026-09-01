"""The write gate: nothing reaches the database unless `live` is set."""

from skyportal_db import SkyPortalWriter, render_plan


class _Source:
    id, ra, dec = "GRB260604C", 224.4, 28.8


class _Point:
    def __init__(self, instrument_id=4, mjd=61195.0):
        self.obj_id, self.instrument_id, self.mjd = "GRB260604C", instrument_id, mjd
        self.filter, self.magsys = "ztfg", "ab"
        self.mag, self.magerr, self.limiting_mag = 19.5, 0.05, 20.1
        self.altdata = {"circex_circular_id": 44834}


async def test_dry_run_plans_but_writes_nothing():
    writer = SkyPortalWriter(live=False)
    await writer.write_source(None, _Source(), [1])
    assert [p["op"] for p in writer.plan] == ["source"]


async def test_photometry_is_grouped_by_instrument():
    """add_external_photometry takes one column-oriented payload per instrument."""
    writer = SkyPortalWriter(live=False)
    n = await writer.write_photometry(None, [_Point(4), _Point(4), _Point(7)], [1])
    assert n == 3
    payloads = [p["payload"] for p in writer.plan]
    assert sorted(p["instrument_id"] for p in payloads) == [4, 7]
    assert sorted(p["n"] for p in payloads) == [1, 2]


async def test_photometry_is_tagged_with_an_origin():
    """origin is part of SkyPortal's dedup index, so it must be set."""
    writer = SkyPortalWriter(live=False)
    await writer.write_photometry(None, [_Point()], [])
    assert writer.plan[0]["payload"]["origin"] == ["circex"]


async def test_writes_are_attributed_to_the_configured_user():
    assert SkyPortalWriter(user_id=42).user_id == 42


def test_render_plan_is_readable():
    assert "source" in render_plan([{"op": "source", "payload": {"id": "X"}}])
