"""Shared fixtures: a fake SkyPortal, plus the vendored GRB 260604C flurry."""

from pathlib import Path
from typing import Any

import pytest

FLURRY = Path(__file__).parent / "fixtures" / "flurry"

# aggregate_event only hands back the extractions it built from as of circex
# 0.2.0. Without them there is no event name to resolve against, so anything
# that exercises resolution end-to-end cannot run.
_actions = pytest.importorskip("circex.bot.skyportal_map").SkyPortalActions
requires_extractions = pytest.mark.skipif(
    not hasattr(_actions, "extractions"),
    reason="needs circex with SkyPortalActions.extractions",
)

# The SVOM event the vendored flurry belongs to, as SkyPortal would hold it.
EVENT = {
    "dateobs": "2026-06-04T20:21:59",
    "aliases": ["SVOM#sb26060404"],
    "localizations": [{"localization_name": "svom_eclairs_1"}],
}


class FakeSkyPortal:
    """Answers resolution GETs from a list of events; records every write.

    Aliases posted through `request` land on the stored event, so a test can show
    that writing the alias makes the next lookup resolve by alias.
    """

    def __init__(self, events: list[dict[str, Any]] | None = None) -> None:
        self.events = [dict(e) for e in (events if events is not None else [EVENT])]
        self.plan: list[dict[str, Any]] = []
        self.enabled = False

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        if path != "/gcn_event":
            return None
        params = params or {}
        events = self.events
        if "partialdateobs" in params:
            needle = str(params["partialdateobs"]).lower()
            events = [
                e
                for e in events
                if str(e["dateobs"]).lower().startswith(needle)
                or any(needle in a.lower() for a in e.get("aliases", []))
            ]
        elif "startDate" in params:
            events = [e for e in events if params["startDate"] <= e["dateobs"] <= params["endDate"]]
        return {"events": events, "totalMatches": len(events)}

    def request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        self.plan.append({"method": method, "path": path, "payload": payload or {}})
        if path.endswith("/alias") and payload:
            dateobs = path.split("/")[2]
            for event in self.events:
                if event["dateobs"] == dateobs:
                    event.setdefault("aliases", []).append(payload["alias"])
        return {}

    def paths(self, needle: str) -> list[dict[str, Any]]:
        return [w for w in self.plan if needle in w["path"]]


@pytest.fixture
def client() -> FakeSkyPortal:
    return FakeSkyPortal()


@pytest.fixture
def cfg() -> dict[str, Any]:
    return {
        "skyportal": {"group_ids": [1988], "default_instrument_id": 4, "instrument_map": {}},
        "resolver": {"order": ["alias", "designation", "trigger"], "window_hours": 12},
        "writes": {
            "live": False,
            "alias": True,
            "source": True,
            "confirm_in_gcn": True,
            "comment": True,
            "tag": True,
        },
        "extractor": {"kind": "regex"},
    }


@pytest.fixture
def flurry() -> Path:
    return FLURRY
