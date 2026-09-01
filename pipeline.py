"""One circular in, SkyPortal writes out.

Extraction lives in `circex`; this module is the binding. For each circular it
reconstructs the whole event from the GCN cross-reference graph, aggregates it
into one counterpart, resolves which SkyPortal GcnEvent that belongs to, and
writes the source and the event-level attachments.

Photometry is not deduplicated here. SkyPortal has a unique index over
(obj_id, instrument_id, origin, mjd, fluxerr, flux) and `add_external_photometry`
resolves collisions against it, so re-seeing an event is idempotent by
construction and survives restarts.

Circex imports are deferred so this module loads without a model server or the
archive.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import gcn_event

log = logging.getLogger("circex_plugin.pipeline")

_MJD_EPOCH = datetime(1858, 11, 17, tzinfo=UTC)


def mjd_to_iso(mjd: float) -> str:
    return (_MJD_EPOCH + timedelta(days=mjd)).isoformat()


def build_extractor(cfg: dict[str, Any]) -> Any:
    """regex | llama | hybrid, per config."""
    ecfg = cfg.get("extractor") or {}
    kind = (ecfg.get("kind") or "regex").lower()

    from circex.extract.regex.extractor import RegexExtractor

    classifier = None
    if model_path := ecfg.get("sn_type_model"):
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
        require_fields=bool(ecfg.get("llama_require_fields", False)),
        api_key=ecfg.get("llama_api_key") or None,
    )
    if kind == "llama":
        return llm
    if kind == "hybrid":
        from circex.extract.hybrid import HybridExtractor

        # Keep the trained classifier as a fallback when the LLM abstains; the
        # default routing drops regex classification as noise.
        overrides = {"classification": ("llm", "regex")} if classifier else None
        return HybridExtractor(regex, llm, routing_overrides=overrides)
    raise ValueError(f"unknown extractor kind {kind!r} (expected regex, llama, or hybrid)")


def event_names(actions: Any) -> list[str]:
    """Candidate event names, designations first.

    A counterpart name (AT2017gfo) never resolves an event, so designations sort
    ahead of it; both are still written as aliases.
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
        lines.append(
            f"Counterpart: {actions.source.id} at RA={actions.source.ra}, Dec={actions.source.dec}"
        )
    if actions.photometry:
        bands = sorted({p.filter for p in actions.photometry})
        lines.append(f"Photometry: {len(actions.photometry)} point(s) in {', '.join(bands)}")
    if actions.redshift is not None:
        z, z_err = actions.redshift
        lines.append(f"Redshift: z = {z}" + (f" +/- {z_err}" if z_err is not None else ""))
    for label in _classifications(actions):
        lines.append(f"Classification: {label}")
    for note in dict.fromkeys(n for e in actions.extractions for n in e.extraction_meta.notes):
        lines.append(f"Note: {note}")
    if actions.extractions:
        lines.append(f"Extractor: {actions.extractions[0].extraction_meta.extractor}")
    return "\n".join(lines)


def _classifications(actions: Any) -> list[str]:
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
    photometry: int = 0
    status: str = "nothing-postable"  # posted | unresolved-event | nothing-postable
    names: list[str] = field(default_factory=list)


async def process_circular(
    record: dict[str, Any],
    *,
    session: Any,
    extractor: Any,
    writer: Any,
    fetch: Any,
    cfg: dict[str, Any],
) -> ProcessResult:
    """Extract one circular's event and bind it to its SkyPortal GcnEvent."""
    from circex.bot.aggregate import aggregate_event, gather_by_xref

    circular_id = int(record.get("circularId") or 0)
    spcfg = cfg.get("skyportal") or {}
    rcfg = cfg.get("resolver") or {}
    writes = cfg.get("writes") or {}
    group_ids = spcfg.get("group_ids") or []

    records = gather_by_xref(circular_id, fetch, max_hops=1)
    actions = aggregate_event(
        records,
        extractor,
        instrument_map=spcfg.get("instrument_map") or {},
        default_instrument_id=spcfg.get("default_instrument_id"),
        group_ids=group_ids,
    )
    result = ProcessResult(circular_id=circular_id)
    if actions.source is None:
        return result
    result.obj_id = actions.source.id

    names = event_names(actions)
    result.names = names
    match = await gcn_event.resolve_event(
        session,
        names=names,
        trigger_time=_trigger_time(records),
        order=rcfg.get("order"),
        window_hours=float(rcfg.get("window_hours") or 12),
    )
    if match is None:
        # The event usually appears within minutes; the caller parks the
        # circular and retries rather than dropping the extraction.
        result.status = "unresolved-event"
        return result
    result.dateobs, result.matched_by = str(match.dateobs), match.matched_by

    if writes.get("source", True):
        await writer.write_source(session, actions.source, group_ids)
        result.photometry = await writer.write_photometry(session, actions.photometry, group_ids)
        if actions.redshift is not None:
            z, z_err = actions.redshift
            await writer.set_redshift(session, actions.source.id, z, z_err)

    await gcn_event.write_event_bindings(
        session,
        writer,
        match,
        names=names,
        obj_id=actions.source.id,
        comment=build_comment(actions, [r.get("circularId") for r in records]),
        detection_window=detection_window(actions.photometry),
        localization_cumprob=float(rcfg.get("localization_cumprob") or 0.95),
        writes=writes,
    )
    result.status = "posted"
    return result


def _trigger_time(records: list[dict[str, Any]]) -> datetime | None:
    """Earliest circular timestamp in the event, as a proxy for the trigger."""
    stamps = []
    for record in records:
        raw = record.get("createdOn") or record.get("trigger_time")
        if raw is None:
            continue
        try:
            when = (
                datetime.fromtimestamp(raw / 1000, tz=UTC)
                if isinstance(raw, int | float)
                else datetime.fromisoformat(str(raw))
            )
        except (ValueError, OSError, OverflowError):
            continue
        stamps.append(when if when.tzinfo else when.replace(tzinfo=UTC))
    return min(stamps) if stamps else None
