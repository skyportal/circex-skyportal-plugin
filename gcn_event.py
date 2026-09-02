"""Bind a Circex extraction to a SkyPortal GcnEvent.

Two halves. `resolve_event` turns the event name a circular writes ("GRB 260604C",
"S260604a") into the `dateobs` SkyPortal keys events by, trying progressively
looser rungs. `write_event_bindings` then attaches the extraction to that event:
the name as an alias, the optical counterpart as a source confirmed in the GCN,
and the parts that have no structured home as a comment.

Resolution rungs, in config order:

  alias       a substring match against GcnEvent.aliases, so "S260604a" finds
              the LVC#S260604a the notice ingester wrote.
  trigger_id  an identifier in the circular text matching GcnEvent.trigger_id,
              e.g. SVOM's "burst-id sb26060404".
  designation GRB/GW/EP/SVOM names encode their own UTC date; search that day.
  trigger     the circular's own trigger_time, +/- window_hours.

The alias write is what makes rung 1 hit next time: the first circular of an
event usually resolves by designation, and every later one by alias.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
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
    re.compile(r"(?i)\bIC(\d{6})[A-Z]?\b"),  # IceCube
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


async def _events_in_window(session, centre, hours):
    import sqlalchemy as sa
    from skyportal.models import GcnEvent
    from sqlalchemy.orm import selectinload

    return (
        (
            await session.scalars(
                sa.select(GcnEvent)
                .where(
                    GcnEvent.dateobs >= centre - timedelta(hours=hours),
                    GcnEvent.dateobs <= centre + timedelta(hours=hours),
                )
                .options(selectinload(GcnEvent.localizations))
            )
        )
        .unique()
        .all()
    )


async def _event_by_trigger_id(session, text):
    """The event whose trigger_id appears verbatim in the circular.

    Stronger than any date arithmetic: the notice and the circular are naming
    the same trigger. Only reasonably distinctive ids are tried, since a short
    numeric one would match digits anywhere in the prose.
    """
    import sqlalchemy as sa
    from skyportal.models import GcnEvent
    from sqlalchemy.orm import selectinload

    candidates = {t for t in re.findall(r"\b[A-Za-z0-9_-]{6,}\b", text)}
    if not candidates:
        return None
    return await session.scalar(
        sa.select(GcnEvent)
        .where(GcnEvent.trigger_id.in_(candidates))
        .options(selectinload(GcnEvent.localizations))
    )


async def _events_by_alias(session, needle):
    """Events whose aliases contain `needle`, ignoring case and spaces."""
    import sqlalchemy as sa
    from skyportal.models import GcnEvent
    from sqlalchemy.orm import selectinload

    pattern = f"%{needle.replace(' ', '').lower()}%"
    return (
        (
            await session.scalars(
                sa.select(GcnEvent)
                .where(
                    sa.func.replace(
                        sa.func.lower(sa.cast(GcnEvent.aliases, sa.String)), " ", ""
                    ).like(pattern)
                )
                .options(selectinload(GcnEvent.localizations))
            )
        )
        .unique()
        .all()
    )


def _to_match(event, matched_by):
    return EventMatch(
        dateobs=event.dateobs,
        aliases=list(event.aliases or []),
        localizations=[loc.localization_name for loc in (event.localizations or [])],
        matched_by=matched_by,
    )


def _pick(events, centre):
    """One event out of several, or None when the choice would be a guess.

    A designation fixes the burst's day, and a busy day holds several events, so
    without a time to compare against there is nothing to separate them.
    Attaching a counterpart to the wrong event is worse than not attaching it,
    so an ambiguous day is left for the retry queue rather than resolved.
    """
    if not events:
        return None
    if len(events) == 1:
        return events[0]
    if centre is None:
        log.info(
            "ambiguous event day: %d candidates and no trigger time; not resolving",
            len(events),
        )
        return None
    centre = centre.replace(tzinfo=None)
    return min(events, key=lambda e: abs((e.dateobs - centre).total_seconds()))


async def resolve_event(
    session, *, names, trigger_time=None, order=None, window_hours=12.0, text=""
):
    """First rung that hits wins. None means the event isn't in SkyPortal yet."""
    order = order or ["alias", "trigger_id", "designation", "trigger"]
    for rung in order:
        if rung == "alias":
            for name in names:
                picked = _pick(await _events_by_alias(session, name), trigger_time)
                if picked is not None:
                    return _to_match(picked, f"alias:{name}")
        elif rung == "trigger_id" and text:
            picked = await _event_by_trigger_id(session, text)
            if picked is not None:
                return _to_match(picked, "trigger_id")
        elif rung == "designation":
            for name in names:
                day = parse_designation(name)
                if day is None:
                    continue
                centre = datetime(day.year, day.month, day.day, 12)
                picked = _pick(await _events_in_window(session, centre, 12.0), trigger_time)
                if picked is not None:
                    return _to_match(picked, f"designation:{name}")
        elif rung == "trigger" and trigger_time is not None:
            centre = trigger_time.replace(tzinfo=None)
            picked = _pick(await _events_in_window(session, centre, window_hours), trigger_time)
            if picked is not None:
                return _to_match(picked, "trigger")
    return None


async def write_event_bindings(
    session,
    writer,
    match,
    *,
    names,
    obj_id,
    comment,
    detection_window=None,
    localization_cumprob=0.95,
    position=None,
    writes=None,
):
    """Attach the extraction to the event: aliases, counterpart, comment, tag."""
    writes = writes or {}
    fresh = [n for n in names if n and not _alias_present(match.aliases, n)]
    if fresh and writes.get("alias", True):
        writer._record("alias", {"dateobs": str(match.dateobs), "aliases": fresh})
        if writer.live:
            await _add_aliases(session, match.dateobs, fresh)

    if obj_id is not None and writes.get("confirm_in_gcn", True):
        # sources_in_gcn confirms a source against a localization, so an event
        # with no skymap yet cannot take the association. Circular-only events
        # have none, so synthesize a cone from the reported position first.
        if not match.localizations and position is not None and writes.get("localization", True):
            await ensure_localization(session, writer, match, *position)
        if not match.localizations:
            log.info("event %s has no localization; skipping sources_in_gcn", match.dateobs)
        elif detection_window is None:
            log.info("no detection window for %s; skipping sources_in_gcn", obj_id)
        else:
            writer._record(
                "sources_in_gcn",
                {
                    "dateobs": str(match.dateobs),
                    "source_id": obj_id,
                    "localization_name": match.localizations[0],
                    "localization_cumprob": localization_cumprob,
                },
            )
            if writer.live:
                await _confirm_source(session, match, obj_id, localization_cumprob)

    if comment and writes.get("comment", True):
        writer._record("comment", {"dateobs": str(match.dateobs), "text": comment})
        if writer.live:
            await _add_comment(session, match.dateobs, comment, writer.user_id)


# Bounds on a synthesized localization. Widening is safe and narrowing is not, so
# the floor clamps but the ceiling refuses: an IPN annulus stated as 99.7 deg
# across is not an ellipse, and shrinking it to fit would claim a precision the
# circular never gave.
MIN_LOCALIZATION_ERROR_DEG = 0.01
MAX_LOCALIZATION_ERROR_DEG = 10.0
DEFAULT_LOCALIZATION_ERROR_DEG = 0.05


def cone_radius(ra_dec_error):
    """Circex's ra_dec_error [deg] as a usable radius, or None if unrepresentable.

    An ellipse arrives as [semi-major, semi-minor, position angle]; the radius is
    the semi-major axis, the first element. The third is an angle, not a radius.
    """
    error = ra_dec_error
    if isinstance(error, list):
        error = next((e for e in error if isinstance(e, int | float)), None)
    if not isinstance(error, int | float) or error <= 0:
        error = DEFAULT_LOCALIZATION_ERROR_DEG
    if float(error) > MAX_LOCALIZATION_ERROR_DEG:
        return None
    return max(float(error), MIN_LOCALIZATION_ERROR_DEG)


def localization_shape(ra_dec_error):
    """How to draw the region: an ellipse when both axes are known, else a cone.

    IPN error boxes are long and thin (2.45 deg by 19 arcmin is typical), so a
    circle enclosing one would be far larger than the region actually reported.
    Returns None when the region is too large to represent.
    """
    axes = (
        [e for e in ra_dec_error if isinstance(e, int | float) and e > 0]
        if isinstance(ra_dec_error, list)
        else []
    )
    if len(axes) >= 2:
        amaj, amin = axes[0], axes[1]
        if amaj > MAX_LOCALIZATION_ERROR_DEG:
            return None
        phi = axes[2] if len(axes) > 2 else 0.0
        return {
            "kind": "ellipse",
            "amaj": max(amaj, MIN_LOCALIZATION_ERROR_DEG),
            "amin": max(amin, MIN_LOCALIZATION_ERROR_DEG),
            "phi": float(phi),
        }
    error = cone_radius(ra_dec_error)
    return None if error is None else {"kind": "cone", "error": error}


async def ensure_localization(session, writer, match, ra, dec, ra_dec_error=None):
    """Give an event with no skymap a cone localization from the circular position.

    Most events get a localization from a Notice. A circular-only event (a
    Konus-Wind burst, say) arrives with none, and without one the source cannot
    be associated with the event at all. Returns the localization name, or None.
    """
    if match.localizations:
        return match.localizations[0]
    if ra is None or dec is None:
        return None

    shape = localization_shape(ra_dec_error)
    if shape is None:
        log.info(
            "stated localization for %s is too large to represent; not synthesizing",
            match.dateobs,
        )
        return None
    writer._record(
        "localization",
        {"dateobs": str(match.dateobs), "ra": ra, "dec": dec, **shape},
    )
    if not writer.live:
        return None

    import asyncio

    import sqlalchemy as sa
    from skyportal.handlers.api.gcn import add_tiles_and_properties_and_contour
    from skyportal.models import Localization
    from skyportal.utils.gcn import from_cone, from_ellipse

    if shape["kind"] == "ellipse":
        amaj, amin, phi = shape["amaj"], shape["amin"], shape["phi"]
        name = f"circex_{ra:.5f}_{dec:.5f}_{amaj:.5f}_{amin:.5f}"
        skymap = from_ellipse(name, ra, dec, amaj, amin, phi)
    else:
        skymap = from_cone(ra=ra, dec=dec, error=shape["error"])
    skymap["dateobs"] = match.dateobs
    name = skymap["localization_name"]

    existing = await session.scalar(
        sa.select(Localization).where(
            Localization.dateobs == match.dateobs,
            Localization.localization_name == name,
        )
    )
    if existing is not None:
        return name

    localization = Localization(**skymap)
    session.add(localization)
    await session.commit()
    log.info("created localization %s for %s", name, match.dateobs)

    # Tiles are what sources_in_gcn queries, so they have to exist before the
    # association is attempted. Deliberately not the obsplan variant — a cone
    # derived from a circular should not queue observing plans.
    try:
        await asyncio.to_thread(
            add_tiles_and_properties_and_contour,
            localization.id,
            writer.user_id,
            None,
            None,
            False,
        )
    except Exception as exc:
        log.warning("could not tile localization %s: %s", name, exc)
        return None

    match.localizations.insert(0, name)
    return name


def _alias_present(aliases, name):
    needle = name.replace(" ", "").lower()
    return any(needle in str(a).replace(" ", "").lower() for a in aliases)


async def _add_aliases(session, dateobs, names):
    import sqlalchemy as sa
    from skyportal.models import GcnEvent
    from sqlalchemy.orm.attributes import flag_modified

    event = await session.scalar(sa.select(GcnEvent).where(GcnEvent.dateobs == dateobs))
    if event is None:
        return
    event.aliases = list(event.aliases or []) + list(names)
    flag_modified(event, "aliases")


async def _add_comment(session, dateobs, text, user_id):
    import sqlalchemy as sa
    from skyportal.models import CommentOnGCN, GcnEvent

    event = await session.scalar(sa.select(GcnEvent).where(GcnEvent.dateobs == dateobs))
    if event is None:
        return
    session.add(CommentOnGCN(text=text, gcn_id=event.id, author_id=user_id, bot=True))


async def _confirm_source(session, match, obj_id, cumprob):
    import sqlalchemy as sa
    from skyportal.models import SourcesConfirmedInGCN

    existing = await session.scalar(
        sa.select(SourcesConfirmedInGCN).where(
            SourcesConfirmedInGCN.dateobs == match.dateobs,
            SourcesConfirmedInGCN.obj_id == obj_id,
        )
    )
    if existing is not None:
        return
    session.add(
        SourcesConfirmedInGCN(
            dateobs=match.dateobs,
            obj_id=obj_id,
            confirmed=True,
            localization_name=match.localizations[0],
            localization_cumprob=cumprob,
        )
    )
