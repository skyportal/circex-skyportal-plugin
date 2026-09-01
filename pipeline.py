"""One circular in, SkyPortal writes out.

Extraction itself lives in `circex`; this module is the binding. For each
circular it reconstructs the whole event from the GCN cross-reference graph,
aggregates it into one counterpart, resolves which SkyPortal GcnEvent that
belongs to, and writes both the source and the event-level attachments.

Circex imports are deliberately deferred to `build_extractor` and
`process_circular` so the module imports — and the tests run — without a model
server, the archive, or the unreleased parts of circex.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import gcn_event

log = logging.getLogger("circex_plugin.pipeline")

# MJD epoch, for turning photometry times back into the ISO strings the
# sources_in_gcn endpoint wants.
_MJD_EPOCH = datetime(1858, 11, 17, tzinfo=UTC)

# A point counts as already present if the source has one in the same filter
# within this many days. Matches circex's consumer: the same stacked exposure is
# reported at its start/mid/end epoch across circulars, minutes apart, and an
# exact-mjd key would duplicate those.
DEDUP_MJD_TOL = 0.02


def mjd_to_iso(mjd: float) -> str:
    return (_MJD_EPOCH + timedelta(days=mjd)).isoformat()


def build_extractor(cfg: dict[str, Any]) -> Any:
    """regex | llama | hybrid, per config. Raises if the kind is unknown."""
    ecfg = cfg.get("extractor") or {}
    kind = (ecfg.get("kind") or "regex").lower()

    from circex.extract.regex.extractor import RegexExtractor

    classifier = None
    model_path = ecfg.get("sn_type_model")
    if model_path:
        from circex.classify.sn_type import SNTypeClassifier

        classifier = SNTypeClassifier.load(Path(model_path))

    regex = RegexExtractor(sn_classifier=classifier) if classifier else RegexExtractor()
    if kind == "regex":
        return regex

    cache = None
    if cache_path := ecfg.get("cache_path"):
        from circex.cache.llm import LLMCache

        Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
        cache = LLMCache(Path(cache_path))

    from circex.extract.llm.llama_server import LlamaServerExtractor

    llm = LlamaServerExtractor(
        base_url=ecfg.get("llama_url") or "http://localhost:8080",
        model_id=ecfg.get("llama_model") or "mistral-7b",
        cache=cache,
        timeout=float(ecfg.get("llama_timeout") or 300),
    )
    if kind == "llama":
        return llm
    if kind == "hybrid":
        from circex.extract.hybrid import HybridExtractor

        # A trained classifier on the regex side is worth keeping when the LLM
        # abstains; the default routing drops regex classification as noise.
        overrides = {"classification": ("llm", "regex")} if classifier else None
        return HybridExtractor(regex, llm, routing_overrides=overrides)
    raise ValueError(f"unknown extractor kind {kind!r} (expected regex, llama, or hybrid)")


def event_names(actions: Any) -> list[str]:
    """Candidate event names for resolution, most specific designation first.

    A counterpart name (AT2017gfo) never resolves an event, so designations sort
    ahead of it; both are still written as aliases. aggregate_event stamps the
    fused name onto every extraction, so any of them carries the full set.
    """
    names: list[str] = []
    for extraction in getattr(actions, "extractions", ()):
        event = getattr(extraction, "event", None)
        raw = getattr(event, "event_name", None) if event is not None else None
        if raw is None:
            continue
        names.extend(n for n in (raw if isinstance(raw, list) else [raw]) if n)
    return sorted(dict.fromkeys(names), key=lambda n: gcn_event.parse_designation(n) is None)


def detection_window(points: list[Any], pad_days: float = 1.0) -> tuple[str, str] | None:
    """ISO bracket around the counterpart's detections, for sources_in_gcn."""
    mjds = [p.mjd for p in points if getattr(p, "mjd", None) is not None]
    if not mjds:
        return None
    return mjd_to_iso(min(mjds) - pad_days), mjd_to_iso(max(mjds) + pad_days)


def build_comment(actions: Any, circular_ids: list[int]) -> str:
    """The event-level comment: what was extracted, and from which circulars."""
    lines = ["Circex extraction from GCN circular(s) " + ", ".join(str(i) for i in circular_ids)]
    if actions.source is not None:
        source = actions.source
        lines.append(f"Counterpart: {source.id} at RA={source.ra}, Dec={source.dec}")
    if actions.photometry:
        bands = sorted({p.filter for p in actions.photometry})
        lines.append(f"Photometry: {len(actions.photometry)} point(s) in {', '.join(bands)}")
    if actions.redshift is not None:
        z, z_err = actions.redshift
        lines.append(f"Redshift: z = {z}" + (f" +/- {z_err}" if z_err is not None else ""))
    for classification in _classifications(actions):
        lines.append(f"Classification: {classification}")
    for note in dict.fromkeys(
        note for e in actions.extractions for note in e.extraction_meta.notes
    ):
        lines.append(f"Note: {note}")
    if actions.extractions:
        lines.append(f"Extractor: {actions.extractions[0].extraction_meta.extractor}")
    return "\n".join(lines)


def _classifications(actions: Any) -> list[str]:
    """Distinct classification labels across the event's circulars, order preserved."""
    labels = []
    for extraction in actions.extractions:
        classification = getattr(extraction, "classification", None)
        name = getattr(classification, "class_name", None) if classification else None
        if name:
            labels.append(str(name))
    return list(dict.fromkeys(labels))


@dataclass
class ProcessResult:
    circular_id: int
    obj_id: str | None = None
    dateobs: str | None = None
    matched_by: str = ""
    photometry_posted: int = 0
    photometry_skipped: int = 0
    status: str = "nothing-postable"  # posted | unresolved-event | nothing-postable
    names: list[str] = field(default_factory=list)


# Session memory: (obj_id, filter) -> mjds already posted or already in SkyPortal.
SeenPhotometry = dict[tuple[str, str], list[float]]


@dataclass
class SessionState:
    """What this run has already written, so re-seeing an event is a no-op.

    Every circular of a flurry names the same event, so without this the alias
    and the summary comment would be re-posted a dozen times over. Photometry is
    deduped by (obj_id, filter, mjd) within a tolerance; the rest by identity.
    """

    photometry: SeenPhotometry = field(default_factory=dict)
    aliases: set[tuple[str, str]] = field(default_factory=set)  # (dateobs, alias)
    tags: set[str] = field(default_factory=set)  # dateobs
    commented: set[str] = field(default_factory=set)  # dateobs
    sources: set[tuple[str, float | None, float | None]] = field(default_factory=set)

    def is_duplicate(self, point: Any) -> bool:
        mjds = self.photometry.get((point.obj_id, point.filter))
        return mjds is not None and any(abs(m - point.mjd) <= DEDUP_MJD_TOL for m in mjds)

    def remember(self, point: Any) -> None:
        self.photometry.setdefault((point.obj_id, point.filter), []).append(point.mjd)


def _write_source(
    client: Any, actions: Any, dateobs: str | None, state: SessionState | None
) -> None:
    """Source upsert, photometry, redshift patch — stamped with the event.

    The upsert is skipped once the source has been posted at this position; a
    refined position later in the flurry still re-posts.
    """
    source = actions.source
    key = (source.id, source.ra, source.dec)
    if state is None or key not in state.sources:
        client.request("POST", "/sources", source.to_payload())
        if state is not None:
            state.sources.add(key)
    obj_id = source.id
    for point in actions.photometry:
        payload = point.to_payload()
        if dateobs is not None:
            payload.setdefault("altdata", {})["gcn_dateobs"] = dateobs
        client.request("POST", "/photometry", payload)
    if actions.redshift is not None:
        z, z_err = actions.redshift
        client.request("PATCH", f"/sources/{obj_id}", {"redshift": z, "redshift_error": z_err})


def process_circular(
    record: dict[str, Any],
    *,
    extractor: Any,
    client: Any,
    fetch: Any,
    cfg: dict[str, Any],
    state: SessionState | None = None,
) -> ProcessResult:
    """Extract one circular's event and bind it to its SkyPortal GcnEvent."""
    from circex.bot.aggregate import aggregate_event, gather_by_xref

    circular_id = int(record.get("circularId") or 0)
    spcfg = cfg.get("skyportal") or {}
    rcfg = cfg.get("resolver") or {}
    writes = cfg.get("writes") or {}

    records = gather_by_xref(circular_id, fetch, max_hops=1)
    actions = aggregate_event(
        records,
        extractor,
        instrument_map=spcfg.get("instrument_map") or {},
        default_instrument_id=spcfg.get("default_instrument_id"),
        group_ids=spcfg.get("group_ids") or [],
    )
    result = ProcessResult(circular_id=circular_id)

    if actions.source is None:
        return result
    result.obj_id = actions.source.id

    names = event_names(actions)
    result.names = names
    match = gcn_event.resolve_event(
        client,
        names=names,
        trigger_time=_trigger_time(records),
        order=rcfg.get("order"),
        window_hours=float(rcfg.get("window_hours") or 12),
    )
    if match is None:
        # The event usually exists in SkyPortal within minutes; the caller parks
        # the circular and retries rather than dropping the extraction.
        result.status = "unresolved-event"
        return result
    result.dateobs, result.matched_by = match.dateobs, match.matched_by

    if state is not None:
        fresh = []
        for point in actions.photometry:
            if state.is_duplicate(point):
                continue
            fresh.append(point)
            state.remember(point)
        result.photometry_skipped = len(actions.photometry) - len(fresh)
        actions = replace(actions, photometry=fresh)

    if writes.get("source", True):
        _write_source(client, actions, match.dateobs, state)
    result.photometry_posted = len(actions.photometry)

    # A flurry names the same event over and over; only say each thing once.
    dateobs = match.dateobs
    fresh_names = names
    comment: str | None = None
    if state is not None:
        fresh_names = [n for n in names if (dateobs, n) not in state.aliases]
        state.aliases.update((dateobs, n) for n in fresh_names)
        first_sighting = dateobs not in state.commented
        if first_sighting or result.photometry_posted:
            comment = build_comment(actions, [r.get("circularId") for r in records])
            state.commented.add(dateobs)
        if dateobs in state.tags:
            writes = {**writes, "tag": False}
        elif writes.get("tag", False):
            state.tags.add(dateobs)
    else:
        comment = build_comment(actions, [r.get("circularId") for r in records])

    gcn_event.write_event_bindings(
        client,
        match,
        names=fresh_names,
        obj_id=actions.source.id,
        comment=comment,
        detection_window=detection_window(actions.photometry),
        localization_cumprob=float(rcfg.get("localization_cumprob") or 0.95),
        writes=writes,
    )
    result.status = "posted"
    return result


def _trigger_time(records: list[dict[str, Any]]) -> datetime | None:
    """Earliest circular timestamp in the event — a proxy for the trigger epoch."""
    stamps = []
    for record in records:
        raw = record.get("createdOn") or record.get("trigger_time")
        if raw is None:
            continue
        try:
            # GCN's createdOn is epoch milliseconds; a trigger_time is ISO.
            when = (
                datetime.fromtimestamp(raw / 1000, tz=UTC)
                if isinstance(raw, int | float)
                else datetime.fromisoformat(str(raw))
            )
        except (ValueError, OSError, OverflowError):
            continue
        stamps.append(when if when.tzinfo else when.replace(tzinfo=UTC))
    return min(stamps) if stamps else None
