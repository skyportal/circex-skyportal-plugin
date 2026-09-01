"""Bind a Circex extraction to a SkyPortal GcnEvent.

Two halves. `resolve_event` turns the event name a circular writes ("GRB 260604C",
"S260604a") into the `dateobs` SkyPortal keys events by, trying progressively
looser rungs. `write_event_bindings` then attaches the extraction to that event:
the name as an alias, the optical counterpart as a source confirmed in the GCN,
and the parts that have no structured home as a comment.

Resolution rungs, in config order:

  alias       GET /gcn_event?partialdateobs=<name> — matches a dateobs prefix OR
              a substring of the event's aliases, so "S260604a" finds the
              LVC#S260604a written by the notice ingester.
  designation GRB/GW/EP/SVOM names encode their own UTC date; search that day.
  trigger     the circular's own trigger_time, +/- window_hours.

The alias write is what makes rung 1 hit next time: the first circular of an
event usually resolves by designation, and every later one by alias.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

log = logging.getLogger("circex_plugin.gcn_event")

# Designations that carry their own UTC date as YYMMDD. The trailing letter
# (GRB 260604C, S260604a) orders bursts within a day and is not part of the date.
_DESIGNATION_PATTERNS = (
    re.compile(r"(?i)\bGRB[\s_-]?(\d{6})[A-Z]?\b"),
    re.compile(r"(?i)\bGW[\s_-]?(\d{6})[A-Z]?\b"),
    re.compile(r"(?i)\bS(\d{6})[a-z]{1,2}\b"),  # LVK superevent
    re.compile(r"(?i)\bEP(\d{6})[a-z]?\b"),
    re.compile(r"(?i)\bSVOM[\s_-]?(\d{6})[A-Z]?\b"),
)

# Two-digit year pivot. The GCN archive starts in 1997, so 90+ is last century.
_YEAR_PIVOT = 90


def parse_designation(name: str) -> date | None:
    """UTC date encoded in a GRB/GW/EP/SVOM designation, or None."""
    for pattern in _DESIGNATION_PATTERNS:
        match = pattern.search(name)
        if match is None:
            continue
        digits = match.group(1)
        yy, mm, dd = int(digits[:2]), int(digits[2:4]), int(digits[4:6])
        year = (1900 + yy) if yy >= _YEAR_PIVOT else (2000 + yy)
        try:
            return date(year, mm, dd)
        except ValueError:  # e.g. a 6-digit trigger id that isn't a date
            continue
    return None


@dataclass(frozen=True)
class EventMatch:
    """A SkyPortal GcnEvent this circular belongs to."""

    dateobs: str
    aliases: list[str] = field(default_factory=list)
    localizations: list[dict[str, Any]] = field(default_factory=list)
    matched_by: str = ""

    @property
    def localization_name(self) -> str | None:
        """Newest localization on the event — what sources_in_gcn confirms against."""
        return self.localizations[0].get("localization_name") if self.localizations else None


def _events_from(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    events = (payload or {}).get("events")
    return events if isinstance(events, list) else []


def _to_match(event: dict[str, Any], matched_by: str) -> EventMatch | None:
    dateobs = event.get("dateobs")
    if not dateobs:
        return None
    return EventMatch(
        dateobs=str(dateobs),
        aliases=list(event.get("aliases") or []),
        localizations=list(event.get("localizations") or []),
        matched_by=matched_by,
    )


def _search_window(client: Any, centre: datetime, window_hours: float) -> list[dict[str, Any]]:
    return _events_from(
        client.get(
            "/gcn_event",
            {
                "startDate": (centre - timedelta(hours=window_hours)).isoformat(),
                "endDate": (centre + timedelta(hours=window_hours)).isoformat(),
                "numPerPage": 50,
            },
        )
    )


def _pick(events: list[dict[str, Any]], centre: datetime | None) -> dict[str, Any] | None:
    """One event out of a window. Nearest in time to `centre` when we have one."""
    if not events:
        return None
    if centre is None or len(events) == 1:
        return events[0]

    def distance(event: dict[str, Any]) -> float:
        try:
            when = datetime.fromisoformat(str(event["dateobs"])).replace(tzinfo=UTC)
        except (KeyError, ValueError):
            return float("inf")
        return abs((when - centre).total_seconds())

    return min(events, key=distance)


def resolve_event(
    client: Any,
    *,
    names: list[str],
    trigger_time: datetime | None = None,
    order: list[str] | None = None,
    window_hours: float = 12.0,
) -> EventMatch | None:
    """First rung that hits wins. None means the event isn't in SkyPortal yet."""
    order = order or ["alias", "designation", "trigger"]
    for rung in order:
        if rung == "alias":
            for name in names:
                # Query both spellings: circulars write "GRB 260604C", notices
                # and TACH write "GRB260604C".
                for spelling in dict.fromkeys([name, name.replace(" ", "")]):
                    events = _events_from(
                        client.get("/gcn_event", {"partialdateobs": spelling, "numPerPage": 50})
                    )
                    picked = _pick(events, trigger_time)
                    if picked is not None:
                        return _to_match(picked, f"alias:{spelling}")
        elif rung == "designation":
            for name in names:
                day = parse_designation(name)
                if day is None:
                    continue
                centre = datetime(day.year, day.month, day.day, 12, tzinfo=UTC)
                picked = _pick(_search_window(client, centre, 12.0), trigger_time or centre)
                if picked is not None:
                    return _to_match(picked, f"designation:{name}")
        elif rung == "trigger" and trigger_time is not None:
            picked = _pick(_search_window(client, trigger_time, window_hours), trigger_time)
            if picked is not None:
                return _to_match(picked, "trigger")
    return None


def write_event_bindings(
    client: Any,
    match: EventMatch,
    *,
    names: list[str],
    obj_id: str | None,
    comment: str | None,
    detection_window: tuple[str, str] | None = None,
    localization_cumprob: float = 0.95,
    writes: dict[str, Any] | None = None,
) -> None:
    """Attach the extraction to the event: aliases, counterpart, comment, tag."""
    writes = writes or {}
    dateobs = match.dateobs

    if writes.get("alias", True):
        for name in names:
            if name and name not in match.aliases:
                client.request("POST", f"/gcn_event/{dateobs}/alias", {"alias": name})

    if obj_id is not None and writes.get("confirm_in_gcn", True):
        # sources_in_gcn confirms a source *against a localization*, so an event
        # with no skymap yet (many GRB circulars) cannot take the association.
        localization_name = match.localization_name
        if localization_name is None:
            log.info("event %s has no localization; skipping sources_in_gcn", dateobs)
        elif detection_window is None:
            log.info("no detection window for %s; skipping sources_in_gcn", obj_id)
        else:
            start, end = detection_window
            client.request(
                "POST",
                f"/sources_in_gcn/{dateobs}",
                {
                    "source_id": obj_id,
                    "confirmed": True,
                    "localization_name": localization_name,
                    "localization_cumprob": localization_cumprob,
                    "start_date": start,
                    "end_date": end,
                },
            )

    if comment and writes.get("comment", True):
        client.request("POST", f"/gcn_event/{dateobs}/comments", {"text": comment})

    if obj_id is not None and writes.get("tag", False):
        tag = writes.get("counterpart_tag", "HAS-OPTICAL-COUNTERPART")
        client.request("POST", f"/gcn_event/{dateobs}/tags", {"text": tag})
